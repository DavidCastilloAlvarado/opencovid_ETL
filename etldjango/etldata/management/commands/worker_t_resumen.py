from django.core.management.base import BaseCommand, CommandError
from etldjango.settings import GOOGLE_APPLICATION_CREDENTIALS, GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT
from .utils.storage import Bucket_handler, GetBucketData
from .utils.extractor import Data_Extractor
from datetime import datetime, timedelta
from .utils.unicodenorm import normalizer_str
from etldata.models import DB_sinadef, DB_minsa_muertes, DB_vacunas, DB_uci, DB_positividad_relativa, DB_resumen
from django.contrib.gis.geos import Point
# from django.utils import timezone
from django.db.models import Sum, Avg, Count, StdDev
from tqdm import tqdm
import pandas as pd
import numpy as np
import os
import time
# datetime.now(tz=timezone.utc)  # you can use this value


class Command(BaseCommand):
    help = "RESUMEN: Command fore create a resumen using the current date in the DB"

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
                last_date = str(last_record[0].fecha_corte.date())
            else:
                last_date = '2020-05-01'
            table = table.loc[table.fecha_corte > last_date]
            if len(table):
                self.print_shell("Storing new records")
                records = table.to_dict(orient='records')
                records = [db(**record) for record in tqdm(records)]
                _ = db.objects.bulk_create(records)
            else:
                self.print_shell("No new data was found to store")

    def handle(self, *args, **options):
        self.print_shell("Computing covid19 resume from db")
        # Downloading data from bucket
        deads_before_d, deads_before_d_std = self.query_avg_daily_deads_before_covid(
            DB_sinadef)
        deads_after, count = self.query_total_deads_sinadef(DB_sinadef)
        self.deads_sinadef = self.calculate_sinadef(deads_before_d,
                                                    deads_after, count,
                                                    deads_before_d_std)
        self.deads_minsa = self.query_deads_minsa(DB_minsa_muertes)
        self.vaccinated = self.query_vaccinated_first_dosis(DB_vacunas)
        self.camas_uci_disp = self.query_camas_uci_disponible(DB_uci)
        self.active_cases = self.query_active_cases(DB_positividad_relativa)
        self.save_resume(DB_resumen)
        self.print_shell("Work Done!")

    def save_resume(self, db):
        data = dict(fallecidos_sinadef=self.deads_sinadef,
                    fallecidos_minsa=self.deads_minsa,
                    vacunados=self.vaccinated,
                    camas_uci_disp=self.camas_uci_disp,
                    active_cases=self.active_cases,)
        _ = db.objects.create(**data)

    def query_avg_daily_deads_before_covid(self, db):
        min_fecha_hist = datetime.strptime("01-01-18", "%d-%m-%y")
        max_fecha_hist = datetime.strptime("01-01-20", "%d-%m-%y")
        daily_deads = db.objects.values('fecha').filter(fecha__gt=min_fecha_hist,
                                                        fecha__lt=max_fecha_hist,
                                                        region='PERU')
        daily_deads = daily_deads.annotate(Sum('n_muertes'))
        daily_deads = daily_deads.aggregate(Avg('n_muertes__sum'),
                                            StdDev('n_muertes__sum'))

        print(daily_deads)
        return daily_deads['n_muertes__sum__avg'], daily_deads['n_muertes__sum__stddev']

    def query_total_deads_sinadef(self, db):
        max_fecha_hist = datetime.strptime("01-03-20", "%d-%m-%y")
        total_deads = db.objects.values('fecha').filter(fecha__gt=max_fecha_hist,
                                                        region='PERU')
        total_deads = total_deads.annotate(Sum('n_muertes'))
        total_deads = total_deads.aggregate(Sum('n_muertes__sum'),
                                            Count('n_muertes__sum'))
        print(total_deads)
        return total_deads['n_muertes__sum__sum'],  total_deads['n_muertes__sum__count']

    def calculate_sinadef(self, daily_after, total_before, count, std):
        std = pow(float((std**2)*count), .5)
        mean_total_deads = float(total_before - count*daily_after)
        result_min = mean_total_deads - std
        result_max = mean_total_deads + std
        print(result_min, result_max)
        return mean_total_deads

    def query_deads_minsa(self, db):
        query = db.objects.values('fecha').filter(region='PERU')
        query = query.annotate(Sum('n_muertes'))
        query = query.aggregate(Sum('n_muertes__sum'),
                                Count('n_muertes__sum'))
        print(query)
        return query['n_muertes__sum__sum']

    def query_vaccinated_first_dosis(self, db):
        query = db.objects.values('dosis').filter(dosis=1)
        query = query.aggregate(Count('dosis'))
        print(query)
        return query['dosis__count']

    def query_camas_uci_disponible(self, db):
        query = db.objects.aggregate(Sum('serv_uci_left'),
                                     Sum('serv_uci_total'))
        print(query)
        return query['serv_uci_left__sum']  # , query['serv_uci_total__sum']

    def query_active_cases(self, db):
        min_date = str(datetime.now().date() - timedelta(days=14))
        query = db.objects.filter(fecha__gt=min_date).aggregate(Sum('total'))
        print(query)
        return query['total__sum']
