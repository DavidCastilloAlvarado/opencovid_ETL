import os
from django.core.management.base import BaseCommand, CommandError
from etldjango.settings import GOOGLE_APPLICATION_CREDENTIALS, GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT
from .utils.storage import GetBucketData
from .utils.extractor import Data_Extractor
from datetime import datetime, timedelta
from etldata.models import DB_sinadef, Logs_extractor
from .utils.unicodenorm import normalizer_str
# from django.utils import timezone
from tqdm import tqdm
import pandas as pd
import numpy as np
# datetime.now(tz=timezone.utc)  # you can use this value


class Command(BaseCommand):
    help = "SINADEF: Command for transform the tables and upload to the data base"
    bucket = GetBucketData(project_id=GCP_PROJECT_ID)
    file_name = "sinadef.csv"

    def add_arguments(self, parser):
        parser.add_argument(
            'mode', type=str, help="full/last , full: the whole external dataset. last: only the latest records")

    def handle(self, *args, **options):
        mode = options["mode"]
        assert mode in ['full', 'last'], "Error in --mode argument"
        self.print_shell("SINADEF transformation working ....")
        self.downloading_data_from_bucket()
        table = self.read_file_and_format_date()
        table = self.filter_date_and_deads(table, mode)
        table = self.format_columns_name(table)
        table = self.transforma_sinadef_table(table)
        table = self.transform_sinadef_roller_deads_total(table)
        self.save_table(table, DB_sinadef, mode)
        self.print_shell("Work Done!")

    def print_shell(self, text):
        self.stdout.write(self.style.SUCCESS(text))

    def downloading_data_from_bucket(self,):
        last_record = Logs_extractor.objects.filter(status='ok',
                                                    mode='upload',
                                                    e_name=self.file_name)[:1]
        last_record = list(last_record)
        assert len(last_record) > 0, "There are not any file {} in the bucket".format(
            self.file_name)
        last_record = last_record[0]
        source_url = last_record.url
        print(source_url)
        self.bucket.get_from_bucket(source_name=source_url,
                                    destination_name='temp/'+self.file_name)

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

    def read_file_and_format_date(self):
        col_extr = [
            "DEPARTAMENTO DOMICILIO",
            "PROVINCIA DOMICILIO",
            "MUERTE VIOLENTA",
            "FECHA",
        ]
        sinadef = pd.read_csv("temp/"+self.file_name,
                              sep=";",
                              usecols=col_extr,
                              encoding='latin-1',
                              header=2)  # .iloc[:, 0:31]
        sinadef.FECHA = sinadef.FECHA.apply(
            lambda x: datetime.strptime(x, "%Y-%m-%d"))
        return sinadef

    def filter_date_and_deads(self, table, mode, min_date="2018-01-01"):
        list_ = ["NO SE CONOCE", 'SIN REGISTRO']
        # Filtros
        if mode == 'full':
            # max_date = str(datetime.now().date() - timedelta(days=30)) # test only
            table = table.loc[(table.FECHA >= min_date) &
                              (table["MUERTE VIOLENTA"].isin(list_))]
        elif mode == 'last':
            max_date = table.FECHA.max() - timedelta(days=1)
            min_date = str(datetime.now().date() - timedelta(days=30))
            table = table.loc[(table.FECHA >= min_date) &
                              (table.FECHA <= max_date) &
                              (table["MUERTE VIOLENTA"].isin(list_))]
        return table

    def format_columns_name(self, table):
        table.rename(columns={
            'DEPARTAMENTO DOMICILIO': 'region',
            'PROVINCIA DOMICILIO': 'provincia',
            'FECHA': 'fecha',
        }, inplace=True)
        return table

    def transforma_sinadef_table(self, table):
        table = self.getting_lima_region_and_metropol(table)
        table["n_muertes"] = 1
        table = table.groupby(by=['region', 'fecha']).sum().fillna(0)
        table.sort_values(by='fecha', inplace=True)
        return table

    def transform_sinadef_roller_deads_total(self, table, n_roll=7):
        table = table.groupby(["region", ])
        table_roll = pd.DataFrame()
        # Roller mean for every region
        for region in table:
            temp = region[1].sort_values(by="fecha")
            temp = temp.fillna(method="backfill")
            temp["n_muertes_roll"] = temp.rolling(n_roll, center=False).mean()
            temp = temp.dropna()
            table_roll = table_roll.append(temp)
        table_roll.reset_index(inplace=True)
        # Roller mean for the whole country
        temp = table_roll.groupby(["fecha"]).agg({
            "n_muertes": 'sum',
            "n_muertes_roll": 'sum',
        }).reset_index()
        temp["region"] = "PERU"
        table_roll = table_roll.append(temp, ignore_index=True)

        print(table_roll.tail(50))
        print(table_roll.info())
        self.print_shell("Records :{}".format(table_roll.shape))
        return table_roll

    def getting_lima_region_and_metropol(self, table):
        def transform_region(x):
            if x['region'] == 'LIMA':
                if x['provincia'] == 'LIMA':
                    return 'LIMA METROPOLITANA'
                else:
                    return 'LIMA REGION'
            else:
                return x['region']
        table['region'] = table.apply(transform_region, axis=1)
        return table.drop(columns=['provincia'])
