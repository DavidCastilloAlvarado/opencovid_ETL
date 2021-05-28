from django.core.management.base import BaseCommand, CommandError
from .utils.storage import GetBucketData
from .utils.extractor import Data_Extractor
from .utils.urllibmod import urlretrieve
from datetime import datetime, timedelta
from etldata.models import DB_vaccine_arrived
from .utils.unicodenorm import normalizer_str
#from django.utils import timezone
from tqdm import tqdm
import pandas as pd
import numpy as np


class Command(BaseCommand):
    help = "Command for store Vaccines arrived"
    file_name = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSitZm8CsWGbFCGBU_wp6R9uVY9cRscQqXETOuBz61Yjhhr2wA1aNfxCwZAQpwnV46F03BIgAmMhAL1/pub?output=csv"

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
        mode = options["mode"]
        assert mode in ['full', 'last'], "Error in --mode argument"
        table = self.read_raw_data_format_date()
        table = self.format_columns(table)
        table = self.transform_vacunas(table)
        self.save_table(table, DB_vaccine_arrived, mode)
        self.print_shell("Work Done!")

    def read_raw_data_format_date(self,):
        # usecols=cols_extr)
        cols = [
            'fecha',
            'Pfizer',
            'Sinopharm',
            'Astrazeneca',
            'Pfizer Covax',
            'Astrazeneca Covax',
            'total',
        ]
        table = pd.read_csv(self.file_name, usecols=cols)
        # Format date
        table.fecha = table.fecha.apply(
            lambda x: datetime.strptime(str(int(x)), "%Y%m%d") if x == x else x)
        return table


    def format_columns(self, table):
        table.rename(columns={
            'fecha':'fecha',
            'Pfizer':'pfizer',
            'Sinopharm':'sinopharm',
            'Astrazeneca':'astrazeneca',
            'Pfizer Covax':'pfizer_covax',
            'Astrazeneca Covax':'astrazeneca_covax',
            'total':'total',
        }, inplace=True)
        return table

    def transform_vacunas(self, table):
        # normalize words
        table.fillna(0, inplace=True)
        #print(table)
        print(table.sum())
        return table