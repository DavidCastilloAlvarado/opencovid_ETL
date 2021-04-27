from django.core.management.base import BaseCommand, CommandError
from etldjango.settings import GOOGLE_APPLICATION_CREDENTIALS, GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT
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
        months = self.get_months(mode)
        # Downloading data from bucket
        table, total = self.query_vaccinated_first_dosis(DB_vacunas, months)
        table = self.transform_dayli_goals(table, total)
        self.save_table(table, DB_vaccine_resum, mode)
        self.print_shell("Work Done!")

    def get_months(self, mode):
        if mode == 'full':
            return 12
        elif mode == 'last':
            return .5

    def query_vaccinated_first_dosis(self, db, months):
        """
        Record of Vaccinations
        """
        min_date = str(datetime.now().date() - timedelta(days=int(months*30)))
        query = db.objects
        query = query.filter(fecha__gt=min_date, dosis=1)
        query = query.values('fecha')
        query = query.annotate(diario=Sum('cantidad'))
        query = query.order_by('-fecha')
        table = pd.DataFrame.from_dict(query)
        # Total vaccinateds
        query_all = db.objects.filter(dosis=1)
        query_all = query_all.aggregate(total=Sum('cantidad'))
        print(query_all)
        return table, float(query_all['total'])

    def transform_dayli_goals(self, table, total):
        table['acum'] = -table['diario'].cumsum()
        table['acum'] = table['acum'].shift(1).fillna(0)
        table['acum'] = table['acum'].astype(float) + total
        table = self.goals_generator(table)
        print(table.head())
        return table

    def goals_generator(self, table):
        def goal_worker(x):
            left = self.TOTAL_POBLACION - x['acum']
            days_left = datetime(
                *self.FIN_VACUNACION).date() - x['fecha'].date()
            days_left = days_left.days
            daily_goal = round(left/days_left, 1)
            return pd.Series(data=[daily_goal, left], index=['meta', 'resta'])
        return table.join(table.apply(goal_worker, axis=1))
