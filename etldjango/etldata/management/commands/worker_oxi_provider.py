from django.core.management.base import BaseCommand, CommandError
from etldjango.settings import GOOGLE_APPLICATION_CREDENTIALS, GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT
from .utils.storage import GetBucketData
from .utils.health_provider import GetHealthFromGoogleMaps
from .utils.extractor import Data_Extractor
from .utils.urllibmod import urlretrieve
from datetime import datetime, timedelta
from etldata.models import DB_oxi
from .utils.unicodenorm import normalizer_str
from django.db.models import F, Sum, Avg, Count, StdDev, Max, Q
from django.contrib.gis.geos import Point
#from django.utils import timezone
from tqdm import tqdm
import json
import pandas as pd
import numpy as np
from urllib.request import urlopen
import os
import time
import tabula
import re
# datetime.now(tz=timezone.utc)  # you can use this value
URL_MINSA_REPORT = "https://www.dge.gob.pe/portalnuevo/covid-19/covid-cajas/situacion-del-covid-19-en-el-peru/"


class Command(BaseCommand):
    help = "Command for load the oxi provider"
    bucket = GetBucketData(project_id=GCP_PROJECT_ID)
    googleapi = GetHealthFromGoogleMaps()
    oxi_pre_loaded = 'oxigeno_negocio.csv'
    ubigeo = 'ubigeo_gps.csv'

    def add_arguments(self, parser):
        """
        Example:
        """
        parser.add_argument(
            'mode', type=str, help="csv/search , csv: load the data from a csv file. search: to search using the googlemaps API")

    def print_shell(self, text):
        self.stdout.write(self.style.SUCCESS(text))

    def download_csv_from_bucket(self, filename):
        self.print_shell("Downloading csv from bucket ...")
        self.bucket.download_blob(bucket_name=BUCKET_NAME,
                                  source_blob_name="data_source/"+filename,
                                  destination_file_name='temp/'+filename)

    def save_table(self, table, db):
        records = table.to_dict(orient='records')
        records = [db(**record) for record in tqdm(records)]
        _ = db.objects.all().delete()
        _ = db.objects.bulk_create(records)

    def handle(self, *args, **options):
        mode = options["mode"]
        assert mode in ['search', 'csv'], "Error in --mode argument"
        if mode == 'search':
            self.download_csv_from_bucket(self.ubigeo)
            table = self.search_using_googlemaps_api()
        elif mode == 'csv':
            self.download_csv_from_bucket(self.oxi_pre_loaded)
            table = self.read_oxi_preloaded()
        table = self.format_table(table)
        self.save_table(table, DB_oxi)
        self.print_shell("Work Done!")

    def read_ubigeo_gps(self):
        ubigeo2 = pd.read_csv('temp/' + self.ubigeo)
        ubigeo2['location'] = ubigeo2['location'].apply(
            lambda x: json.loads(x.replace("\'", "\"")))
        ubigeo2 = ubigeo2.groupby('NOMBDEP').first().reset_index()
        ubigeo2.head()
        return ubigeo2

    def read_oxi_preloaded(self):
        table = pd.read_csv('temp/'+self.oxi_pre_loaded)
        # print(table.info())
        # print(table.head())
        return table

    def search_using_googlemaps_api(self,):
        ubigeo = self.read_ubigeo_gps()
        whole_places = []
        with tqdm(total=len(ubigeo)) as pbar:
            for location, departamento in zip(ubigeo.location, ubigeo.NOMBDEP):
                places_list = self.googleapi.get_oxi_places_from_points(
                    location, 49000, 'Perú')
                #_ = [place.update({'departamento': departamento}) for place in places_list]
                whole_places = whole_places + places_list
                pbar.update(1)
        print('Cantidad de records ', len(whole_places))
        df = pd.DataFrame.from_records(whole_places)
        df = df.groupby('place_id').agg({'location': 'last',
                                         'name': 'last',
                                        'rating': 'last',
                                         'user_ratings_total': 'last',
                                         'formatted_address': 'last',
                                         'formatted_phone_number': 'last',
                                         'website': 'last',
                                         'negocio': 'sum',
                                         # 'departamento':'last'
                                         }).reset_index()
        df = self.transform_result(df)
        return df

    def transform_result(self, df):
        def field_negocio(x):
            venta = False
            alquiler = False
            recarga = False
            if 'venta' in x['negocio']:
                venta = True
            if 'alquiler' in x['negocio']:
                alquiler = True
            if 'recarga' in x['negocio']:
                recarga = True
            lat = x['location']['lat']
            lng = x['location']['lng']
            return pd.Series(data=[venta, alquiler, recarga, lat, lng], index=['venta', 'alquiler', 'recarga', 'latitude', 'longitude'])

        df = df.join(df.apply(field_negocio, axis=1))
        df.drop(columns=['negocio', 'location'], inplace=True)
        df.to_csv('temp/'+self.oxi_pre_loaded, index=False)
        # df2.head()
        return df

    def format_table(self, table):
        table.rename(columns={
            'place_id': 'place_id',
            'name': 'nombre',
            'rating': 'rating',
            'user_ratings_total': 'n_users',
            'formatted_address': 'direccion',
            'formatted_phone_number': 'telefono',
            'website': 'paginaweb',
            'venta': 'venta',
            'alquiler': 'alquiler',
            'recarga': 'recarga',
        }, inplace=True)
        table["location"] = table.apply(
            lambda x: Point(x['longitude'], x['latitude']), axis=1)
        table.drop(columns=['longitude', 'latitude'], inplace=True)
        print(table.info())
        print(table.head())
        return table
