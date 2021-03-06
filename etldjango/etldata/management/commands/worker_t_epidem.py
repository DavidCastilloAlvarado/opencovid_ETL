from django.core.management.base import BaseCommand, CommandError
from etldjango.settings import GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT
from .utils.storage import Bucket_handler, GetBucketData
from .utils.extractor import Data_Extractor
from datetime import datetime, timedelta
from .utils.unicodenorm import normalizer_str
from etldata.models import DB_minsa_muertes, DB_positividad_salida, DB_capacidad_hosp, DB_minsa_muertes, DB_rt, DB_epidemiologico, DB_vacunas
from django.contrib.gis.geos import Point
# from django.utils import timezone
from django.db.models import F, Sum, Avg, Count, StdDev, Max, Q
from tqdm import tqdm
import pandas as pd
import numpy as np
import os
import time
# datetime.now(tz=timezone.utc)  # you can use this value
#db = DB_epidemiologico.objects.filter(fecha="2021-05-17")
#db.delete()

class Command(BaseCommand):
    help = "Epidemiolog: Command for create the resumen using the current date in the DB"
    bucket = Bucket_handler(project_id=GCP_PROJECT_ID)
    filename = 'poblacion.csv'

    def add_arguments(self, parser):
        """
        Example:
        - for initialize the database using the last three weeks
        $python manage.py worker_t_epidem full --w 3
        - for append the last three weeks
        $python manage.py worker_t_epidem last --w 3
        """
        parser.add_argument(
            'mode', type=str, help="full/last , full: load the last 5 weeks, last: load the last week")
        parser.add_argument(
            '--w', type=int, help="reset the database and load the #w last weeks")

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
                last_date = '2021-01-01'
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
        w = options["w"]
        assert mode in ['full', 'last'], "Error in --mode argument"
        weeks = self.get_weeks_from_args(mode, w)
        self.print_shell("Computing epidemiology score")
        # Downloading data from bucket
        self.downloading_source_csv()
        self.load_poblacion_table_popu()
        table_vacc = self.query_vacunados(DB_vacunas, weeks)
        #
        table_pos = self.query_test_positivos(DB_positividad_salida, weeks)
        table_pos = self.normalizer_100k_population(table_pos,
                                                    ['total', 'total_pos'])
        table_uci = self.query_uci_status(DB_capacidad_hosp, weeks)
        table_minsa = self.query_deaths_minsa(DB_minsa_muertes, weeks)
        table_minsa = self.normalizer_100k_population(table_minsa,
                                                      ['n_muertes'])
        table_rt = self.query_rt_score(DB_rt, weeks)
        table = self.merge_tables(
            table_pos, table_uci, table_minsa, table_rt, table_vacc)
        table = self.aggregate_avg_by_week(table)
        table = self.calc_vacc_progress(table)
        table = self.scoring_variables(table)
        table = self.last_week_comparation(table)
        self.save_table(table, DB_epidemiologico, mode)
        self.print_shell('Work Done!')

    def get_weeks_from_args(self, mode, weeks):
        if weeks:
            return weeks + 2
        elif mode == 'full':
            return 6
        elif mode == 'last':
            return 4

    def downloading_source_csv(self):
        """
        Function to download the csv file which contain all the url and standar names
        for the the data from the goberment, then
        read that file and download all the files form source.
        """
        self.print_shell('Downloading poblacion.csv ... ')
        self.bucket.download_blob(bucket_name=BUCKET_NAME,
                                  source_blob_name="data_source/"+self.filename,
                                  destination_file_name="temp/"+self.filename)

    def load_poblacion_table_popu(self):
        table = pd.read_csv('temp/'+self.filename)
        table.rename(columns={
            'Region': 'region',
            'total': 'poblacion'
        }, inplace=True)
        table.region = table.region.apply(lambda x: normalizer_str(x))
        print(table)
        self.table_popu = table

    def get_fecha_max(self, db, fecha='fecha'):
        query = db.objects.values(fecha)
        query = query.aggregate(Max(fecha))
        query = query[fecha+'__max'].date()
        print(query)
        return query

    def query_test_positivos(self, db, weeks):
        fecha_max = self.get_fecha_max(db)
        fecha_min = fecha_max - timedelta(days=8*weeks)
        query = db.objects.values('fecha',
                                  'region',
                                  'total_test',
                                  'total_pos')
        query = query.filter(fecha__gt=fecha_min)
        query = query.annotate(positividad=(
            F('total_pos') / F('total_test')*100))
        query = query.order_by('region')
        query = pd.DataFrame.from_records(query)
        query.rename(columns={
            'total_test': 'total'
        }, inplace=True)
        print(query)
        return query

    def query_uci_status(self, db, weeks):
        fecha_max = self.get_fecha_max(db, 'fecha_corte')
        fecha_min = fecha_max - timedelta(days=8*weeks)
        query = db.objects.values('fecha_corte',
                                  'region',
                                  )
        query = query.filter(fecha_corte__gt=fecha_min)
        query = query.exclude(region='PERU')
        query = query.annotate(uci_p=F('uci_zc_cama_ocup') /
                               F('uci_zc_cama_total')*100,
                               camas_p=F('uci_znc_cama_ocup') /
                               F('uci_znc_cama_total')*100
                               )
        query = pd.DataFrame.from_records(query)
        query.rename(columns={'fecha_corte': 'fecha'}, inplace=True)
        print(query)
        return query

    def query_deaths_minsa(self, db, weeks):
        columns = ['fecha',
                   'region',
                   'n_muertes', ]
        fecha_max = self.get_fecha_max(db,)
        fecha_min = fecha_max - timedelta(days=8*weeks)
        query = db.objects
        query = query.filter(fecha__gt=fecha_min)
        query = query.exclude(region='PERU')
        query = query.order_by('region')
        query = query.values(*columns)

        query = pd.DataFrame.from_records(query)
        print(query.loc[query.region == 'PUNO'].n_muertes.mean())
        print(query.loc[query.region == 'PUNO'])
        return query

    def query_rt_score(self, db, weeks):
        fecha_max = self.get_fecha_max(db, 'date')
        fecha_min = fecha_max - timedelta(days=8*weeks)
        query = db.objects
        query = query.values('date',
                             'region',
                             'ml')
        query = query.filter(date__gt=fecha_min)
        query = query.exclude(region='PERU')
        query = query.order_by('region')
        query = pd.DataFrame.from_records(query)
        query.rename(columns={'date': 'fecha'}, inplace=True)
        print(query)
        return query

    def normalizer_100k_population(self, table, columns):
        def change_normal(x, column):
            if x['region'] == 'LIMA':
                x['region'] = 'LIMA METROPOLITANA'
            n_pp = self.table_popu.loc[self.table_popu.region == x['region']]
            n_pp = n_pp['poblacion'].tolist()[0]
            return x[column]/n_pp*100000
        for column in columns:
            table[column] = table.apply(change_normal, args=(column,), axis=1)
        print(table.isnull().sum())
        print(table.info())
        return table

    @ staticmethod
    def rename_total_table_columns(table):
        table.rename(columns={
            'total': 'avg_test',
            'total_pos': 'incid_100',
            'n_muertes': 'fall_100',
            'ml': 'rt',
            'uci_p': 'uci',
            'camas_p': 'camas_covid',
            'vacc_acum': 'vacc_acum',
        }, inplace=True)
        return table

    def merge_tables(self, posit, uci, minsa, rt, table_vacc):
        total = posit.merge(uci,
                            on=["fecha", "region"],
                            how="outer")
        total = total.merge(minsa,
                            on=["fecha", "region"],
                            how="outer")
        total = total.merge(rt,
                            on=["fecha", "region"],
                            how="outer")
        total = total.merge(table_vacc,
                            on=["fecha", "region"],
                            how="outer")
        total = total.merge(self.table_popu,
                            on=['region'],
                            how='outer')
        total['n_week'] = total.fecha.apply(lambda x: (x).isocalendar()[1])
        # total['n_week'] = total.fecha.apply(
        #     lambda x: (x+timedelta(days=1)).isocalendar()[1])
        # total['n_week'] =
        # the current week never process
        curr_week = (datetime.now()).isocalendar()[1]
        print('current week ', curr_week)
        total = total.loc[(total['n_week'] > total['n_week'].min())
                          & (total['n_week'] < curr_week)
                          ]
        total = self.rename_total_table_columns(total)
        print(total.isnull().sum())
        cols = total.columns.tolist()
        cols.remove('fecha')
        total[cols] = total[cols].apply(pd.to_numeric, errors='ignore')
        # print(total.sort_values(by='fecha').tail(50))
        # print(total.loc[total.region == 'LIMA METROPOLITANA'])
        # print(posit.loc[posit.region ==
        #       'LIMA METROPOLITANA'].sort_values(by='fecha'))
        return total

    def scoring_variables(self, table):
        print(table.info())
        print(table.head())
        print(table.isnull().sum())
        table = table.fillna(0)
        cut_fall = [-0.01, 2, 5, 7, 1e7]
        cut_uci = [-0.01, 70, 90, 98, 100]
        cut_incid = [-0.01, 80, 100, 120, 1e7]
        cut_rt = [-0.01, .7, 1.1, 1.6, 1e7]
        cut_pos = [-0.01, 11, 15, 20, 1e7]
        cut_test = [-1e7, 34*7, 60*7, 100*7, 1e7]
        color = [1, 2, 3, 4]
        table['fall_score'] = pd.cut(table.fall_100,
                                     cut_fall,
                                     labels=color).astype(int)
        table['uci_score'] = pd.cut(table.uci,
                                    cut_uci,
                                    labels=color).astype(int)
        table['incid_score'] = pd.cut(table.incid_100,
                                      cut_incid,
                                      labels=color).astype(int)
        table['rt_score'] = pd.cut(table.rt,
                                   cut_rt,
                                   labels=color).astype(int)
        table['posit_score'] = pd.cut(table.positividad,
                                      cut_pos,
                                      labels=color).astype(int)
        table['test_score'] = pd.cut(table.avg_test,
                                     cut_test,
                                     labels=color[::-1]).astype(int)

        table['score'], table['val_score'] = self.calculate_score(table)
        print(table.describe())
        return table

    @ staticmethod
    def calculate_score(table):
        cut_score = [0, 31, 38, 43, 1e7]
        color = [1, 2, 3, 4]
        w = [4, 3, 2.5, 2, 1.5, 1]
        result = table['fall_score']*w[0] + table['uci_score']*w[1] + \
            table['incid_score'] * w[2] + table['rt_score']*w[3] + \
            table['posit_score'] * w[4] + table['test_score']*w[5]
        return pd.cut(result, cut_score, labels=color).astype(int), result

    def date_table_factory(self, fechas_orig, region_name):
        min_ = fechas_orig.min()
        max_ = fechas_orig.max()
        totaldatelist = pd.date_range(start=min_, end=max_).tolist()
        totaldatelist = pd.DataFrame(data={"fecha": totaldatelist})
        totaldatelist['region'] = region_name
        return totaldatelist

    def aggregate_avg_by_week(self, table):
        table = table.groupby(["region", ])
        table_acum = pd.DataFrame()
        for region in table:
            region_name = region[0]
            temp = region[1].sort_values(by="fecha")
            totaldatelist = self.date_table_factory(temp.fecha, region_name)
            temp = totaldatelist.merge(temp,
                                       on=["fecha", 'region'],
                                       how="outer",
                                       )

            temp = temp.sort_values(by="fecha")
            temp = temp.reset_index(drop=True)
            temp = temp.fillna(method="ffill")
            temp = temp.fillna(method="bfill")
            temp = temp.dropna()
            temp = temp.groupby(["n_week", "region"]).agg({
                'fecha': 'first',
                'avg_test': 'sum',
                'incid_100': 'sum',
                'positividad': 'mean',
                'uci': 'last',
                'camas_covid': 'last',
                'fall_100': 'sum',
                'rt': 'mean',
                'vacc_acum': 'max',
                'poblacion': 'last',
            })
            temp = temp.reset_index()
            # temp.fecha = temp.fecha.apply(lambda x: x.date())
            table_acum = table_acum.append(temp, ignore_index=True)
        # print(table_acum.info())
        table_acum['rt'] = table_acum['rt'].astype(float)
        # print(table_acum.head())
        # print(table_acum.tail(12))
        # print(table_acum.head(12))
        return table_acum

    def calc_vacc_progress(self, table):
        table['vacc_prog'] = table.vacc_acum/table.poblacion*100
        return table

    def query_vacunados(self, db, weeks):
        fecha_max = self.get_fecha_max(db,)
        fecha_min = fecha_max - timedelta(days=8*weeks)
        # Records diarios por region
        query = db.objects
        query = query.filter(dosis=1)
        histo = query.values('fecha', 'region')
        histo = histo.annotate(vacc_acum=Sum('cantidad'))
        histo = histo.order_by('fecha', 'region')
        histo = pd.DataFrame.from_records(histo)
        # .groupby(level=0).cumsum()
        histo = histo.groupby(['region', 'fecha']).sum().astype(float)\
            .groupby(level=0).cumsum()
        histo = histo.reset_index()
        histo['fecha2'] = histo.fecha.apply(lambda x: x.date())
        histo = histo.loc[histo.fecha2 > fecha_min]
        print(histo.columns)
        histo.drop(columns=['fecha2', ], inplace=True)
        # Cantidad total por region
        # # Generando el total
        # print(histo.tail())
        # print(histo.info())
        return histo

    def last_week_comparation(self, table):
        table = table.set_index(['region', 'n_week', 'fecha'])
        temp = table[['incid_100', 'fall_100']]
        temp = temp.groupby(['region', 'n_week', 'fecha']).sum()\
            .groupby(level=0).diff()
        temp.rename(columns={
            'incid_100': 'incid_100_chg',
            'fall_100': 'fall_100_chg',
        }, inplace=True)
        table = table.join(temp)
        table = table.dropna()
        table = table.reset_index()
        print(table.tail())
        return table
