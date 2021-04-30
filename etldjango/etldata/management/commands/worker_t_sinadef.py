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
    file_population = 'poblacion_edad.csv'

    def add_arguments(self, parser):
        parser.add_argument(
            'mode', type=str, help="full/last , full: the whole external dataset. last: only the latest records")

    def handle(self, *args, **options):
        mode = options["mode"]
        assert mode in ['full', 'last'], "Error in --mode argument"
        self.print_shell("SINADEF transformation working ....")
        # self.downloading_data_from_bucket()
        self.download_csv_from_bucket_data_source(self.file_population)
        self.load_population_table()
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

    def download_csv_from_bucket_data_source(self, filename):
        self.print_shell("Downloading csv from bucket ...")
        self.bucket.download_blob(bucket_name=BUCKET_NAME,
                                  source_blob_name="data_source/"+filename,
                                  destination_file_name='temp/'+filename)

    def load_population_table(self):
        table = pd.read_csv('temp/'+self.file_population)
        self.age_cols = table.columns.tolist()
        self.age_cols.remove('total')
        self.age_cols.remove('region')
        self.popu_total_age = table[self.age_cols].sum(0)
        self.population = table

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
            'TIEMPO EDAD',
            'EDAD',
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
        max_date = table.FECHA.max() - timedelta(days=1)
        # Filtros
        if mode == 'full':
            # max_date = str(datetime.now().date() - timedelta(days=30)) # test only
            table = table.loc[(table.FECHA >= min_date) &
                              (table.FECHA <= max_date) &
                              (table["MUERTE VIOLENTA"].isin(list_))]
        elif mode == 'last':
            min_date = str(datetime.now().date() - timedelta(days=30))
            table = table.loc[(table.FECHA >= min_date) &
                              (table.FECHA <= max_date) &
                              (table["MUERTE VIOLENTA"].isin(list_))]
        return table

    def format_columns_name(self, table):
        table.rename(columns={
            'DEPARTAMENTO DOMICILIO': 'region',
            'PROVINCIA DOMICILIO': 'provincia',
            'EDAD': 'edad',
            'TIEMPO EDAD': 'tiempo',
            'FECHA': 'fecha',
        }, inplace=True)
        return table

    def transforma_sinadef_table(self, table):
        table = self.getting_lima_region_and_metropol(table)
        edad_cut = [0, 19, 29, 49, 69, 79, 130]
        self.labels_age = ['age0_19', 'age20_29', 'age30_49',
                           'age50_69', 'age70_79', 'age80_m']
        table['tiempo'] = table['tiempo'].apply(
            lambda x: normalizer_str(x).lower())
        table['edad'] = pd.to_numeric(table['edad'], errors='coerce')
        print(table.head())
        table['edad'] = table.apply(
            lambda x: x['edad'] if x['tiempo'] == 'años' else 1, axis=1)
        table['edad'] = pd.cut(table['edad'], edad_cut,
                               labels=self.labels_age)
        table["n_muertes"] = 1
        table = pd.pivot_table(table, values='n_muertes', index=['region', 'fecha'],
                               columns='edad', aggfunc=np.sum).fillna(0)
        table.columns = self.labels_age
        table['n_muertes'] = table[self.labels_age].sum(1)
        # table.reset_index(inplace=True)
        #table = table.groupby(by=['region', 'fecha']).sum().fillna(0)
        table.sort_values(by='fecha', inplace=True)
        # print(table.head())
        return table

    def normalize_10k_popu(self, table, region_name=None, region=True):
        if region and not region_name in ['EXTRANJERO', 'SIN REGISTRO']:
            vector = self.population.loc[self.population.region == region_name]
            vector = vector[self.age_cols]
            vector = 1/vector*10000
        else:
            vector = 1/self.popu_total_age*10000
        # print(table[self.age_cols])
        table[self.age_cols] = np.multiply(
            table[self.age_cols], np.asarray(vector))
        return table

    def transform_sinadef_roller_deads_total(self, table, n_roll=7):
        table = table.groupby(["region", ])
        table_roll = pd.DataFrame()
        cols_roll_after = self.labels_age + ['n_muertes']
        cols_roll_before = self.labels_age + ["n_muertes_roll"]
        # Roller mean for every region
        for region in table:
            region_name = region[0]
            temp = region[1].sort_values(by="fecha")
            temp = temp.fillna(method="backfill")
            temp = self.normalize_10k_popu(temp, region_name)
            temp[cols_roll_before] = temp[cols_roll_after].rolling(
                n_roll, center=False).mean()
            temp = temp.dropna()
            table_roll = table_roll.append(temp)
        table_roll.reset_index(inplace=True)
        # Roller mean for the whole country
        temp = table_roll.copy()
        temp.drop(columns=['region'], inplace=True)
        temp = temp.groupby(["fecha"]).agg('sum').reset_index()
        temp = self.normalize_10k_popu(table=temp, region=False)
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
