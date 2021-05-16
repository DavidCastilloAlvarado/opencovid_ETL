from django.core.management.base import BaseCommand, CommandError
from etldjango.settings import GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT
from .utils.storage import GetBucketData
from .utils.extractor import Data_Extractor
from .utils.urllibmod import urlretrieve
from datetime import datetime, timedelta
from etldata.models import DB_positividad
from .utils.unicodenorm import normalizer_str
from django.db.models import F, Sum, Avg, Count, StdDev, Max, Q
#from django.utils import timezone
from tqdm import tqdm
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
    help = "Command downloads pdf report from Minsa, for positive cases"
    bucket = GetBucketData(project_id=GCP_PROJECT_ID)
    filename = 'test_positivos.csv'

    def add_arguments(self, parser):
        """
        Example:
        - for initialize the database
        $python manage.py worker_posit csv
        - for load the last pdf from minsa report
        $python manage.py worker_posit pdf
        - for load a particular daily report from minsa %d%m%y
        $python manage.py worker_posit pdf --day 230321
        - update database from any day automatically
        $python manage.py worker_posit pdf --update yes
        """
        parser.add_argument(
            'mode', type=str, help="csv/pdf , csv: load the data from a csv file. pdf: load the data from Minsa's webpage")
        parser.add_argument(
            '--day', type=str, help="put a date time to try to download %d%m%y")
        parser.add_argument(
            '--update', type=str, help="yes: to update from the last recors to the last pdf available")

    def print_shell(self, text):
        self.stdout.write(self.style.SUCCESS(text))

    def save_table(self, table, db, mode):
        if mode == 'csv':
            records = table.to_dict(orient='records')
            records = [db(**record) for record in tqdm(records)]
            _ = db.objects.all().delete()
            _ = db.objects.bulk_create(records)
        elif mode == 'pdf':
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

    def handle(self, *args, **options):
        mode = options["mode"]
        day = options["day"]
        update = options["update"]
        assert mode in ['pdf', 'csv'], "Error in --mode argument"
        if mode == 'pdf':
            urls = self.get_pdfurl_from_webpage(URL_MINSA_REPORT, day, update)
            for url in urls:
                self.check_if_already_exist_in_db(url)
                try:
                    filename, fecha_pdf = self.download_pdf_from_web(url)
                except:
                    self.print_shell(
                        "Url: {} can't be downloaded!".format(url))
                    continue
                self.uploading_pdf_to_bucket(filename, fecha_pdf)
                self.read_pdf_validate(filename)
                table = self.extracting_table_from_pdf(filename)
                table = self.formating_table(table, fecha_pdf)
                self.save_table(table, DB_positividad, mode)
                self.print_shell("Record {} loaded in db!".format(fecha_pdf))
        elif mode == 'csv':
            self.download_csv_from_bucket()
            table = self.loading_csv_dataset_pre()
            self.save_table(table, DB_positividad, mode)

        self.print_shell("Work Done!")

    def check_if_already_exist_in_db(self, url):
        ind = url.index(".pdf")
        fecha_pdf = datetime.strptime(url[(ind-6):ind], "%d%m%y")
        if DB_positividad.objects.filter(fecha=fecha_pdf).exists():
            raise 'The record "fecha" already exist'
        self.print_shell('The file passed to download ... ')

    def get_fecha_max(self, db, fecha='fecha'):
        query = db.objects.values(fecha)
        query = query.aggregate(Max(fecha))
        query = query[fecha+'__max'].date()
        print(query)
        return query

    def get_pdfurl_from_webpage(self, url, day=None, update=None):
        self.print_shell('Getting minsa report url ... ')
        html = urlopen(url)
        text = html.read()
        plaintext = text.decode('utf8')
        links = re.findall("href=[\"\'](.*?)[\"\']", plaintext)
        pdf = []
        for link in links:
            if ".pdf" in link and "coronavirus" in link:
                pdf.append(link)
        if day:
            return [self.changing_date_to_pdf_url(pdf[0], day)]
        elif update:
            assert update == 'yes', 'you only can use "yes" in this field'
            return self.get_urls_to_download(pdf[0])

        self.print_shell(url)
        return [pdf[0]]

    def get_urls_to_download(self, url):
        last_date = self.get_fecha_max(DB_positividad,) + timedelta(days=1)
        today = datetime.now()
        list_dates = pd.date_range(start=last_date, end=today).tolist()
        list_dates = [day.strftime("%d%m%y") for day in list_dates]
        list_dates = [self.changing_date_to_pdf_url(
            url, day) for day in list_dates]
        print(list_dates)
        return list_dates

    def changing_date_to_pdf_url(self, url, day):
        end = url.index(".pdf")
        init = end-6
        url1 = url[:init]
        url2 = url[end:]
        return url1 + day + url2

    def download_pdf_from_web(self, url):
        self.print_shell('Downloading pdf ... ')
        # Geting datatime from name in url
        ind = url.index(".pdf")
        fecha_pdf = datetime.strptime(url[(ind-6):ind], "%d%m%y")
        # Downloading from url
        filename, _ = urlretrieve(url, "temp/minsareport.pdf")
        return filename, fecha_pdf

    def uploading_pdf_to_bucket(self, filename, fecha):
        self.print_shell('Uploading pdf to bucket ... ')
        # Uploading pdf to bucket
        TODAY = str(fecha.date())  # datetime.now().date()
        destination = BUCKET_ROOT + "/report_minsa/" + TODAY + "/" + filename
        self.bucket.upload_blob(bucket_name=BUCKET_NAME,
                                source_file_name=filename,
                                destination_blob_name=destination)

    def read_pdf_validate(self, filename, page=3, contain="MUESTRAS TOTALES"):
        self.print_shell('Validating pdf estructure ... ')
        # Reading and transforming
        tables_raw = tabula.read_pdf(filename, pages=[page])
        alldata = tables_raw[0].sum(1).apply(
            lambda x: str(x).replace("  ", " "))
        assert len(alldata.loc[alldata.str.contains(
            contain)]) > 0, "Table not found"

    def extracting_table_from_pdf(self, filename, page=3, area=(80, 5, 500, 1000)):
        # Extract data
        self.print_shell('Extracting table from pdf... ')
        table = tabula.read_pdf(filename,
                                pages=[page],
                                guess=False,
                                area=area)
        table = table[0].set_index("Región")
        table = table.applymap(lambda x: str(
            x).replace(" ", "").replace(",", "").replace(".", ""))
        table = table.apply(pd.to_numeric, errors='ignore')
        assert "PCR" in table.columns.tolist(), "Doesn't contain PCR column"
        table.reset_index(inplace=True)
        # print(table)
        return table

    def formating_table(self, table, fecha):
        self.print_shell('Formating table to store in db ... ')
        table.drop(columns=["Región.1"], inplace=True)
        table["fecha"] = fecha
        print(table.dtypes)
        table.rename(columns={"Región": "region",
                              "PCR": "pcr_total",
                              "PR": "pr_total",
                              "AG": "ag_total",
                              "PCR.1": "pcr_pos",
                              "Total": "total",
                              "PR.1": "pr_pos",
                              "AG.1": "ag_pos",
                              "Total.1": "total_pos",
                              "% Positividad": "positividad",
                              "mypos": "positividad_verif",
                              }, inplace=True)
        table.drop(columns=["positividad"], inplace=True)
        table = self.formating_region_column(table)
        index = table.loc[table.region == "TOTAL"].index[0]
        table.drop(index=[index], inplace=True)
        print(table.head(50))
        print(table.dtypes)
        return table

    def download_csv_from_bucket(self):
        self.print_shell("Downloading csv from bucket ...")
        self.bucket.download_blob(bucket_name=BUCKET_NAME,
                                  source_blob_name="data_source/"+self.filename,
                                  destination_file_name='temp/'+self.filename)

    def loading_csv_dataset_pre(self):
        # The file already have the format column
        table = pd.read_csv('temp/'+self.filename,)
        table = self.formating_region_column(table)
        print(table.info())
        table = table.fillna(0)
        print(table.region.unique())
        return table

    def formating_region_column(self, table):
        table.region = table.region.apply(lambda x: normalizer_str(x).upper())
        # Lima METROPOLITANA instead of Lima
        table.region = table.region.apply(
            lambda x: "LIMA METROPOLITANA" if x == "LIMA" else x)
        return table
