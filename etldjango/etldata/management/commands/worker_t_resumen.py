from django.core.management.base import BaseCommand, CommandError
from etldjango.settings import GOOGLE_APPLICATION_CREDENTIALS, GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT
from .utils.storage import Bucket_handler, GetBucketData
from .utils.extractor import Data_Extractor
from datetime import datetime, timedelta
from .utils.unicodenorm import normalizer_str
from etldata.models import DB_sinadef, DB_minsa_muertes, DB_vacunas, DB_uci, DB_positividad_relativa, DB_resumen
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
    help = "RESUMEN: Command for create the resumen using the current date in the DB"
    URL_TOTAL_VACUNAS = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vSitZm8CsWGbFCGBU_wp6R9uVY9cRscQqXETOuBz61Yjhhr2wA1aNfxCwZAQpwnV46F03BIgAmMhAL1/pub?output=csv'

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
        deaths_before_d, deaths_before_d_std = self.query_avg_daily_deaths_before_covid(
            DB_sinadef)
        self.total_deaths_subreg = self.query_total_deaths_sinadef_total_subreg(
            DB_sinadef, deaths_before_d)
        deaths_after = self.query_avg7d_deaths_sinadef(DB_sinadef)
        self.deaths_sinadef = self.calculate_subreg_deaths(deaths_before_d,
                                                           deaths_after,
                                                           deaths_before_d_std)
        self.deaths_minsa = self.query_deaths_minsa(DB_minsa_muertes)
        self.avg_vacc_day, self.allvacc = self.query_vaccinated_first_dosis(
            DB_vacunas)
        self.vacc_prog, self.vacc_end, self.vacc_status_goal = self.vacc_forecast()
        self.camas_uci_disp = self.query_camas_uci_disponible(DB_uci)
        self.active_cases = self.query_active_cases(DB_positividad_relativa)
        table = self.read_vacc_total()
        self.val_total_vacc_pe = self.take_total_vacc_pe(table)
        self.save_resume(DB_resumen)
        self.print_shell("Work Done!")

    def save_resume(self, db):
        """
        All metrics are avg in the last 7 days,
        except camas_uci_disp, wich is the last day.
        """
        data = dict(total_fallecidos_sinadef=self.total_deaths_subreg,
                    fallecidos_sinadef=self.deaths_sinadef,
                    fallecidos_minsa=self.deaths_minsa,
                    vacunados=self.avg_vacc_day,
                    totalvacunados1=self.allvacc,
                    vacc_progress=self.vacc_prog,
                    vacc_ends=self.vacc_end,
                    camas_uci_disp=self.camas_uci_disp,
                    active_cases=self.active_cases,
                    vacc_purch_pe=self.val_total_vacc_pe,
                    vacc_day_status_goal=self.vacc_status_goal,
                    )
        print(data)
        _ = db.objects.create(**data)

    def read_vacc_total(self):
        table = pd.read_csv(self.URL_TOTAL_VACUNAS,
                            usecols=['fecha', 'total'])
        table.fecha = table.fecha.apply(
            lambda x: datetime.strptime(str(x), "%Y%m%d"))
        table = table.loc[table.fecha == max(table.fecha)]
        return table

    def take_total_vacc_pe(self, table):
        val = table['total'].tolist()[0]
        return val

    def query_avg_daily_deaths_before_covid(self, db):
        min_fecha_hist = datetime.strptime("01-01-19", "%d-%m-%y")
        max_fecha_hist = datetime.strptime("01-01-20", "%d-%m-%y")
        query = db.objects.values('fecha').filter(fecha__gt=min_fecha_hist,
                                                  fecha__lt=max_fecha_hist,
                                                  region='PERU')
        query = query.annotate(Sum('n_muertes'))
        query = query.aggregate(Avg('n_muertes__sum'),
                                StdDev('n_muertes__sum'))
        print(query)
        query2 = db.objects.values('fecha').filter(fecha__gt=min_fecha_hist,
                                                   fecha__lt=max_fecha_hist,
                                                   region='PERU')

        def median_value(queryset, term):
            count = queryset.count()
            return queryset.values_list(term, flat=True).order_by(term)[int(round(count/2))]

        result = median_value(query2, 'n_muertes')
        print(result)
        # query['n_muertes__sum__avg']
        return result, query['n_muertes__sum__stddev']

    def query_avg7d_deaths_sinadef(self, db):
        """
        AVG deaths by sinadef in the last 7 days
        """
        #max_fecha_hist = datetime.strptime("01-03-20", "%d-%m-%y")
        min_date = str(datetime.now().date() - timedelta(days=30))
        query = db.objects.values('fecha')
        query = query.filter(fecha__gt=min_date,
                             region='PERU')
        query = query.order_by('-fecha')[:7]
        query = query.annotate(Sum('n_muertes'))
        query = query.aggregate(Avg('n_muertes__sum'),
                                Count('n_muertes__sum'))
        print(query)
        return query['n_muertes__sum__avg']

    def query_total_deaths_sinadef_total_subreg(self, db, deaths_before_d):
        min_fecha_hist = datetime.strptime('01-01-20', "%d-%m-%y")
        query = db.objects.values('fecha')
        query = query.filter(fecha__gte=min_fecha_hist,
                             region='PERU')
        query = query.aggregate(n_muertes=Sum(
            'n_muertes'), n_dias=Count('fecha'))
        total = query['n_muertes'] - query['n_dias']*deaths_before_d
        print(query)
        print(total)
        return total

    def calculate_subreg_deaths(self, daily_before, total_after, std):
        """
        AVG deaths in the last 7 days
        """
        mean_total_deaths = float(total_after - daily_before)
        result_min = mean_total_deaths - float(std)
        result_max = mean_total_deaths + float(std)
        print(result_min, result_max)
        return mean_total_deaths

    def query_deaths_minsa(self, db):
        """
        AVG deaths reported by minsa in the last 7 days
        """
        min_date = str(datetime.now().date() - timedelta(days=30))
        query = db.objects
        query = query.filter(region='PERU')
        query = query.filter(fecha__gt=min_date)
        query = query.values('fecha')
        query = query.order_by('-fecha')[:7]
        query = query.annotate(Sum('n_muertes'))
        query = query.aggregate(Avg('n_muertes__sum'),
                                Count('n_muertes__sum'))
        print(query)
        return query['n_muertes__sum__avg']

    def vacc_forecast(self):
        # x29381884x # 22192700 = Poblacion mayor a 18 años - proyección 2019 CPI - censo 2017
        TOTAL_POBLACION = 22192700
        days_left = (TOTAL_POBLACION-int(self.allvacc))/self.avg_vacc_day
        days_left = round(days_left)
        vacc_prog = round(int(self.allvacc)/TOTAL_POBLACION*100, 2)
        vacc_end = datetime.now().date() + timedelta(days=days_left)
        # how have to be the vaccine status in all the country
        init_date_vacc = datetime.strptime("9-02-21", "%d-%m-%y").date()
        end_date_vacc = datetime.strptime("31-12-21", "%d-%m-%y").date()
        curr_date = datetime.now().date()
        num_diff = (curr_date-init_date_vacc).days
        deno_diff = (end_date_vacc-init_date_vacc).days
        vacc_status_goal = num_diff / deno_diff*100

        return vacc_prog, vacc_end, vacc_status_goal

    def query_vaccinated_first_dosis(self, db):
        """
        AVG Vaccinations by day in the last 7 days
        """
        min_date = str(datetime.now().date() - timedelta(days=30))
        query = db.objects
        query = query.filter(fecha__gt=min_date)
        query = query.values('fecha')
        # dosis=1,
        query = query.annotate(Sum('cantidad'))
        query_last = query.order_by('-fecha')[:7]
        #query = query.order_by('-fecha')
        query_last = query_last.aggregate(Avg('cantidad__sum'))
        query_all = db.objects.filter(dosis=1)
        query_all = query_all.aggregate(Sum('cantidad'))
        print(query_all)
        return query_last['cantidad__sum__avg'], query_all['cantidad__sum']

    def query_camas_uci_disponible(self, db):
        """
        TOTAL UCI beds available today
        """
        query = db.objects.aggregate(Sum('serv_uci_left'),
                                     Sum('serv_uci_total'))
        print(query)
        return query['serv_uci_left__sum']  # , query['serv_uci_total__sum']

    def query_active_cases(self, db):
        """
        AVG New cases by day (rolling mean 7)
        """
        fecha_max = self.get_fecha_max(db, 'fecha')
        min_date = str(fecha_max - timedelta(days=7))
        query = db.objects.values('fecha')
        query = query.filter(fecha__gt=min_date)
        query = query.order_by('-fecha')
        query = query.annotate(Sum('total'))
        #query = query.filter(fecha__gte=min_date)
        query = query.aggregate(Avg('total__sum'), Count('total__sum'))
        print(query)
        return query['total__sum__avg']

    def get_fecha_max(self, db, fecha='fecha'):
        query = db.objects.values(fecha)
        query = query.aggregate(Max(fecha))
        query = query[fecha+'__max'].date()
        print(query)
        return query
