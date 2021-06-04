from django.core.management.base import BaseCommand, CommandError
from etldjango.settings import GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT, URL_OXIPERU2, URL_OXIPERU2_DT
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
import urllib3
import json
import pandas as pd
import numpy as np
from urllib.request import urlopen
import os
import time
import tabula
import re
# datetime.now(tz=timezone.utc)  # you can use this value

class Command(BaseCommand):
    help = "Command for load the oxi provider"
    bucket = GetBucketData(project_id=GCP_PROJECT_ID)
    googleapi = GetHealthFromGoogleMaps()
    URL_OXIPERU2 = URL_OXIPERU2
    #URL_OXIGENOPERU = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQyh_QKdlSqO_x6oaNtQ5bFtsqKO4P_lcY1LxO7knnyMI7gsZDvzxgWF2dalzYyP9u2NBKGOxTAhgLl/pub?output=xlsx'
    URL_OXIGENOPERU = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTW3H40DbJl5EPSaznmXeyIhKOPPt2AAvsR7p8gGryRzgMFcSQt3PkScV9uCfvmSAp6KxbfPoRRF3j8/pub?output=xlsx'
    filename_oxiperu = 'temp/oxigenoperu.xlsx'
    oxi_pre_loaded = 'oxigeno_negocio.csv'
    ubigeo = 'ubigeo_gps.csv'

    def add_arguments(self, parser):
        """
        Example:
        """
        parser.add_argument(
            'mode', type=str, help="csv/search/oxiperu/oxiperu2 , csv: load the data from a csv file. search: to search using the googlemaps API")

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
        assert mode in ['search', 'csv', 'oxiperu',
                        'oxiperu2'], "Error in --mode argument"
        if mode == 'search':
            self.download_csv_from_bucket(self.ubigeo)
            table = self.search_using_googlemaps_api()
        elif mode == 'csv':
            self.download_csv_from_bucket(self.oxi_pre_loaded)
            table = self.read_oxi_preloaded()
        elif mode == 'oxiperu':
            self.download_from_oxigeno_peru()
            table = self.raw_data_form_xlsx_oxigeno_peru()
            table = self.format_columns_oxigeno_peru(table)
            #table = self.adding_gps_point_oxigeno_peru(table)
            table = self.format_table_oxigeno_peru(table)
        elif mode == 'oxiperu2':
            table = self.load_data_oxiperu2()
            table = self.transform_oxiperu2(table)
            #table = self.format_table(table)
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

    def download_from_oxigeno_peru(self,):
        handler = Data_Extractor(url=self.URL_OXIGENOPERU,
                                 name=self.filename_oxiperu, many=False)
        handler.extract_one()

    def raw_data_form_xlsx_oxigeno_peru(self,):
        departamentos = ['Lima', 'Junín', 'Puno',
                         'Huánuco', 'Piura', 'Ica',
                         'Ancash', 'Cusco', 'Lambayeque',
                         'La Libertad', 'Ayacucho', 'Arequipa', 'Moquegua']
        columns = ['Nombre', 'Efectivo', 'Tarjeta', 'Transferencia',
                   'Horario', 'Telefono', 'Precio por m3',
                   'Venta', 'Alquiler', 'Recarga', 'Venta.1', 'Alquiler.1',
                   'Direccion', 'latitude', 'longitude', 'Observacion']
        columns_lima = ['Nombre', 'Efectivo', 'Tarjeta', 'Transferencia',
                        'Horario', 'Telefono', 'Precio por m3',
                        'venta', 'alquiler', 'recarga', 'Venta', 'Alquiler',
                        'Direccion', 'latitude', 'longitude', 'Observacion']
        data = pd.DataFrame()
        for departamento in tqdm(departamentos):
            if departamento == 'Lima':
                cols = columns_lima
            else:
                cols = columns
            temp_dep = pd.read_excel(
                self.filename_oxiperu, sheet_name=departamento, header=1, engine='openpyxl')
            temp_dep.columns = [normalizer_str(
                col) for col in temp_dep.columns.tolist()]
            temp_dep = temp_dep[cols]
            temp_dep.columns = columns
            temp_dep['departamento'] = departamento
            temp_dep.dropna(subset=['Nombre'], inplace=True)
            # print(temp_dep.columns.tolist())
            # print(temp_dep)
            data = data.append(temp_dep, ignore_index=True)
        # print(data.head())
        return data

    def format_columns_oxigeno_peru(self, table):
        table.columns = [col.lower() for col in table.columns.tolist()]
        # columns = ['nombre', 'efectivo', 'tarjeta', 'transferencia',
        #            'horario', 'telefono', 'precio por m3',
        #            'venta', 'alquiler', 'recarga', 'venta.1', 'alquiler.1',
        #            'direccion']
        table.rename(columns={'precio por m3': 'precio_m3',
                              'venta.1': 'concent_venta',
                              'alquiler.1': 'concent_alquiler',
                              'Observacion': 'observacion', },
                     inplace=True)
        print(table.info())
        return table

    # def adding_gps_point_oxigeno_peru(self, table):
    #     def get_gps(x, self):
    #         if x['direccion'] == x['direccion']:
    #             len_words = len(x['direccion'].split(' '))
    #             if len_words > 1:
    #                 point = self.googleapi.get_gps(
    #                     str(x['direccion']) + ', ' + x['departamento'])
    #             else:
    #                 point = {}
    #         else:
    #             point = {}
    #         if len(point) == 0:
    #             point = self.googleapi.get_gps(
    #                 x['nombre'] + ', ' + x['departamento'])
    #         print(point)
    #         return pd.Series(point)
    #     temp = table.apply(get_gps, args=(self,), axis=1)
    #     print(temp)
    #     table = table.join(temp)
    #     print(table.head())
    #     print(table.info())
    #     return table

    def format_table_oxigeno_peru(self, table):
        bool_cols = ['venta', 'alquiler', 'recarga',
                     'concent_venta', 'concent_alquiler',
                     'efectivo', 'tarjeta', 'transferencia',
                     ]

        def change_to_bool(x):
            if x == x:
                text = str(x).strip().lower()
                if text == 'si' or text == 'sí':
                    return True
                elif text == 'no':
                    return False
            return False

        def price_handler(x):
            text = {}
            if x == x and not x is None:
                x = str(x).split('/')[-1].upper()
                if 'GRAT' in x:
                    text['m3'] = 'GRATIS'
                elif '-' in x:
                    text['m3'] = x
                else:
                    if float(x) > 200:
                        text['bal10'] = float(x)
                    else:
                        text['m3'] = float(x)
            return json.dumps(text)

        table[bool_cols] = table[bool_cols].applymap(
            lambda x: change_to_bool(x))
        table = table.applymap(lambda x: None if x == '-' or x == 'nan' else x)
        table['precio_m3'] = table['precio_m3'].apply(
            lambda x: price_handler(x))
        table.to_csv('temp/oxigeno_peru.csv', index=False)
        table["location"] = table.apply(
            lambda x: Point(x['longitude'], x['latitude']) if x['longitude'] == x['longitude'] else None, axis=1)
        table.drop(columns=['longitude', 'latitude'], inplace=True)
        print(table.info())
        print(table['precio_m3'])
        return table

    def load_data_oxiperu2(self, ):
        http = urllib3.PoolManager()
        response = http.request('GET', self.URL_OXIPERU2)
        data = json.loads(response.data)
        data = pd.DataFrame.from_dict(data['results'])
        return data

    def transform_oxiperu2(self, data):
        def get_services(x):
            venta = False
            alquiler = False
            recarga = False
            if 'SELL' in x['service']:
                venta = True
            if 'RCHG' in x['service']:
                recarga = True
            if 'RENT' in x['service']:
                alquiler = True
            return pd.Series(data=[alquiler, venta, recarga], index=['alquiler', 'venta', 'recarga'])

        def get_coordinates(x):
            #loc = x['point'].replace('\'', '\"')
            #loc = json.loads(loc)
            loc = x['point']['coordinates']
            lng = loc[0]
            lat = loc[1]
            return pd.Series(data=[lng, lat], index=['lng', 'lat'])

        def get_phone(x):
            phones = x['mobile_phone']
            phones = [str(i) for i in phones]
            phones = ' / '.join(phones)
            return pd.Series(data=[phones], index=['telefono'])

        def horario_homog(x):
            horario = x
            horario = horario.replace('Lunes', 'Lun')
            horario = horario.replace('lun', 'Lun')
            horario = horario.replace('L-', 'Lun -')
            horario = horario.replace('Sábado', 'Sab')
            horario = horario.replace('-S', '- Sab')
            horario = horario.replace('Vier', 'Vie')
            horario = horario.replace('-Vie', '- Vie')
            horario = horario.replace(' a ', ' - ')
            horario = horario.replace('a.m.', ' ')
            horario = horario.replace('D:', 'Dom:')
            horario = horario.replace('8:00p.m.', ' 20:00')
            horario = horario.replace('Sab :', 'Sab:')
            horario = horario.replace('|', '-')
            horario = horario.replace(' )', ' 20:00')
            horario = horario.replace('Sab ', 'Sab: ')
            horario = horario.replace('Dom ', 'Dom: ')
            horario = horario.replace('Vie ', 'Vie: ')
            horario = horario.replace('V:', 'Vie:')
            return horario

        def precio_m3(x):
            text = {}
            if x['min_price_m3'] == x['min_price_m3']:
                if x['min_price_m3'] == 0:
                    text.update({"m3": "GRATIS"})
                else:
                    text.update({"m3": x['min_price_m3']})
            if x['max_price_m3'] == x['max_price_m3']:
                text.update({"balon10": x['max_price_m3']})
            # print(text)
            return pd.Series(data=[json.dumps(text)], index=['precio_m3'])
        #data = dataorig.copy()
        data['observacion'] = data['id'].apply(
            lambda x: self.googleapi.get_details(URL_OXIPERU2_DT, x))
        data = data.join(data.apply(get_services, axis=1))
        data = data.drop(columns=['service'])
        data = data.join(data.apply(get_coordinates, axis=1))
        data = data.drop(columns=['point'])
        data = data.join(data.apply(get_phone, axis=1))
        data = data.drop(columns=['mobile_phone'])
        data = data.join(data.apply(precio_m3, axis=1))
        data.rename(columns={'address': 'direccion',
                             'company_name': 'nombre',
                             'day_service': 'horario', }, inplace=True)
        data = data.drop(columns=['updated_at', 'company_id',
                                  'id', 'min_price_m3', 'max_price_m3'])
        data.nombre = data.nombre.apply(lambda x: x.capitalize())
        data.horario = data.horario.apply(lambda x: horario_homog(x))
        data["location"] = data.apply(
            lambda x: Point(x['lng'], x['lat']) if x['lng'] == x['lng'] else None, axis=1)
        data.drop(columns=['lng', 'lat'], inplace=True)
        data.head()
        print(data.info())
        print(data.head())
        return data
