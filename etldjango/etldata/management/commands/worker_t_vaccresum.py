from django.core.management.base import BaseCommand, CommandError
from etldjango.settings import GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT
from .utils.storage import Bucket_handler, GetBucketData
from .utils.extractor import Data_Extractor
from datetime import datetime, timedelta
from .utils.unicodenorm import normalizer_str
from etldata.models import DB_vaccine_resum, DB_vacunas
from django.contrib.gis.geos import Point
# from django.utils import timezone
from django.db.models import Sum, Avg, Count, StdDev, Max
from tqdm import tqdm
import pandas as pd
import numpy as np
import os
import time
# datetime.now(tz=timezone.utc)  # you can use this value


class Command(BaseCommand):
    TOTAL_POBLACION = 22192700  # poblacion apta para la vacuna
    FIN_VACUNACION = (2021, 12, 31)
    file_population = 'total_popu_vacc.csv'
    bucket = GetBucketData(project_id=GCP_PROJECT_ID)
    help = "RESUMEN: Command for create vaccine resume by date and goals"

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
                last_date = '2020-05-01'
            table = table.loc[table.fecha > last_date]
            if len(table):
                self.print_shell("Storing new records")
                records = table.to_dict(orient='records')
                records = [db(**record) for record in tqdm(records)]
                _ = db.objects.bulk_create(records)
            else:
                self.print_shell("No new data was found to store")

    def handle(self, *args, **options):
        self.print_shell("Computing covid19 vaccinations resume from db")
        mode = options["mode"]
        assert mode in ['full', 'last'], "Error in --mode argument"
        self.download_csv_from_bucket_data_source(self.file_population)
        self.load_population_table()
        months = self.get_months(mode)
        # Downloading data from bucket
        table, total = self.query_vaccinated_first_dosis(DB_vacunas, months)
        table = self.transform_resum_vacc_table(table, total)
        # table = self.transform_dayli_goals(table, total)
        self.save_table(table, DB_vaccine_resum, mode)
        self.print_shell("Work Done!")

    def get_months(self, mode):
        if mode == 'full':
            return 12
        elif mode == 'last':
            return .5

    def download_csv_from_bucket_data_source(self, filename):
        self.print_shell("Downloading csv from bucket ...")
        self.bucket.download_blob(bucket_name=BUCKET_NAME,
                                  source_blob_name="data_source/"+filename,
                                  destination_file_name='temp/'+filename)

    def load_population_table(self):
        self.population = pd.read_csv('temp/'+self.file_population)
        #self.age_cols = table.columns.tolist()
        # self.age_cols.remove('total')
        # self.age_cols.remove('region')
        #self.popu_total_age = table[self.age_cols].sum(0)
        print('Total population: ', self.population.total.sum())
        # print(self.population.head())

    def query_vaccinated_first_dosis(self, db, months):
        """
        Record of Vaccinations
        """
        min_date = str(datetime.now().date() - timedelta(days=int(months*30)))
        query = db.objects
        query = query.filter(fecha__gt=min_date, dosis=1)
        query = query.values('fecha', 'region')
        query = query.annotate(diario=Sum('cantidad'))
        query = query.order_by('-fecha', 'region')
        table = pd.DataFrame.from_dict(query)
        # Total vaccinateds
        query_all = db.objects.filter(dosis=1)
        query_all = query_all.values('region')
        query_all = query_all.annotate(total=Sum('cantidad'))
        query_all = query_all.order_by('region')
        table_total = pd.DataFrame.from_dict(query_all)
        #query_all = query_all.aggregate(total=Sum('cantidad'))
        # print(table_total)
        # print(table)
        return table, table_total

    def transform_dayli_goals(self, table, total, region_name):
        total_popu_region = self.population.loc[self.population.region == region_name]
        total_popu_region = total_popu_region['total'].tolist()[0]
        table['acum'] = -table['diario'].cumsum()
        table['acum'] = table['acum'].shift(1).fillna(0)
        table['acum'] = table['acum'].astype(float) + float(total)
        table = self.goals_generator(table, total_popu_region)
        table = table.sort_values(by="fecha")
        table['diario_roll'] = table['diario'].rolling(7).mean()
        # print(table.head())
        return table

    def goals_generator(self, table, total_popu):
        def goal_worker(x):
            left = total_popu - x['acum']
            days_left = datetime(
                *self.FIN_VACUNACION).date() - x['fecha'].date()
            days_left = days_left.days
            daily_goal = round(left/days_left, 1)
            return pd.Series(data=[daily_goal, left], index=['meta', 'resta'])
        return table.join(table.apply(goal_worker, axis=1))

    def date_table_factory(self, fechas_orig, region_name):
        min_ = fechas_orig.min().min()
        max_ = fechas_orig.max().max()
        totaldatelist = pd.date_range(start=min_, end=max_)
        totaldatelist = totaldatelist.tolist()
        totaldatelist = pd.DataFrame(data={"fecha": totaldatelist})
        totaldatelist.sort_values(by="fecha", ascending=False, inplace=True)
        totaldatelist['region'] = region_name
        return totaldatelist

    def transform_resum_vacc_table(self, table, totals):
        table = table.groupby('region')
        table_total = pd.DataFrame()
        for region in table:
            region_name = region[0]
            total = totals.loc[totals.region == region_name]['total']
            total = total.tolist()[0]
            temp = region[1].sort_values(by="fecha", ascending=False)
            dates = self.date_table_factory(table.fecha, region_name)
            temp = dates.merge(temp,
                               on=['region', 'fecha'],
                               how='left').fillna(0)
            temp.drop(columns=['region'], inplace=True)
            temp = self.transform_dayli_goals(temp, total, region_name)
            temp['region'] = region_name
            table_total = table_total.append(temp)
        table_total = table_total.fillna(0)
        print(table_total.tail())
        return table_total
