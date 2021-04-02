from django.core.management.base import BaseCommand, CommandError
from etldjango.settings import GOOGLE_APPLICATION_CREDENTIALS, GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT
from .utils.storage import GetBucketData
from .utils.extractor import Data_Extractor
from .utils.urllibmod import urlretrieve
from datetime import datetime, timedelta
from etldata.models import DB_positividad
from .utils.unicodenorm import normalizer_str
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
        assert mode in ['full', 'last'], "Error in --mode argument"
        url = self.get_pdfurl_from_webpage(URL_MINSA_REPORT)
        self.check_if_already_exist_in_db(url)
        filename, fecha_pdf = self.download_pdf_(url)
        self.uploading_pdf_to_bucket(filename, fecha_pdf)
        self.read_pdf_validate(filename)
        table = self.extracting_table_from_pdf(filename)
        table = self.formating_table(table, fecha_pdf)
        self.save_table(table, DB_positividad, mode)
        # if init == "yes":
        #     self.print_shell("Downloading CSV from bucket and cleaning table")
        #     self.loading_dataset_pre()
        # elif init == "no":
        #     self.print_shell("Downloading PDF")
        #     self.download_transform_pdf(url)
        self.print_shell("Work Done!")

    def check_if_already_exist_in_db(self, url):
        ind = url.index(".pdf")
        fecha_pdf = datetime.strptime(url[(ind-6):ind], "%d%m%y")
        if DB_positividad.objects.filter(fecha=fecha_pdf).exists():
            raise 'The record "fecha" already exist'
        self.print_shell('The file has passed to download ... ')

    def get_pdfurl_from_webpage(self, url):
        self.print_shell('Getting minsa report url ... ')
        html = urlopen(url)
        text = html.read()
        plaintext = text.decode('utf8')
        links = re.findall("href=[\"\'](.*?)[\"\']", plaintext)
        pdf = []
        for link in links:
            if ".pdf" in link and "coronavirus" in link:
                pdf.append(link)
        return pdf[0]

    def download_pdf_(self, url):
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
        TODAY = str(datetime.now().date())
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

    def extracting_table_from_pdf(self, filename, page=3, area=(80, 10, 500, 1000)):
        # Extract data
        self.print_shell('Extracting table from pdf... ')
        table = tabula.read_pdf(filename,
                                pages=[page],
                                guess=False,
                                area=area)
        table = table[0].set_index("Región")
        table = table.applymap(lambda x: str(
            x).replace(" ", "").replace(",", "."))
        table = table.apply(pd.to_numeric, errors='ignore')
        assert "PCR" in table.columns.tolist(), "Doesn't contain PCR column"
        table.reset_index(inplace=True)
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
        table.region = table.region.apply(lambda x: normalizer_str(x).upper())
        # table.region = table.region.apply(lambda x: "LIMA REGION" if x == "LIMA" else x)  # Lima region instead Lima
        index = table.loc[table.region == "TOTAL"].index[0]
        table.drop(index=[index], inplace=True)
        print(table.head(50))
        return table

    # def loading_dataset_pre(self):
    #     filename_local = "temp/positividad.csv"
    #     self.bucket.download_blob(bucket_name=BUCKET_NAME,
    #                               source_blob_name="data_source/feed_rt.csv",
    #                               destination_file_name=filename_local)
    #     data_pre = pd.read_csv(filename_local, sep=";")
    #     data_pre.REGION = data_pre.REGION.apply(
    #         lambda x: normalizer_str(x).upper())
    #     data_pre.drop(columns=["COUNTRY"], inplace=True)
    #     data_pre.rename(columns={"REGION": "region",
    #                              "Fecha": "fecha",
    #                              "cum_pos_pcr": "PCR_pos",
    #                              "cum_pos_zero": "PR_pos",
    #                              "cum_pos_ag": "AG_pos",
    #                              "cum_pos_total": "Total_pos",
    #                              "cum_total_muestras": "Total"}, inplace=True)
    #     cols = ["PCR_pos", "PR_pos", "AG_pos", "Total_pos", "Total"]
    #     data_pre[cols] = data_pre[cols].applymap(
    #         lambda x: str(x).replace(" ", "").replace("\u202f", "").replace(",", ".") if x == x else x)
    #     data_pre = data_pre.apply(pd.to_numeric, errors="ignore")
    #     print(data_pre.dtypes)
    #     data_pre = data_pre.fillna(0)
    #     self.save_table(data_pre, DB_positividad, init="yes")
