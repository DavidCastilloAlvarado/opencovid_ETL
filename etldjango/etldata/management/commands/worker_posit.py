from django.core.management.base import BaseCommand, CommandError
from etldjango.settings import GOOGLE_APPLICATION_CREDENTIALS, GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT
from .utils.storage import Bucket_handler
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
    bucket = Bucket_handler(project_id=GCP_PROJECT_ID)

    def add_arguments(self, parser):
        parser.add_argument(
            'init', type=str, help="yes/no; yes to load the data from source, no for the latest record in minsa webpage")

    def print_shell(self, text):
        self.stdout.write(self.style.SUCCESS(text))

    def save_table(self, table, db, init="no"):
        records = table.to_dict(orient='records')
        records = [db(**record) for record in tqdm(records)]
        if init == "yes":
            _ = db.objects.all().delete()
        _ = db.objects.bulk_create(records)

    def handle(self, *args, **options):
        init = options["init"]
        assert init in ['yes', 'no'], "Error in --init argument"
        self.print_shell("Downloading PDF")
        url = self.get_pdfurl_from_webpage(URL_MINSA_REPORT)
        if init == "yes":
            self.loading_dataset_pre()
        elif init == "no":
            self.download_transform_pdf(url)
        self.print_shell("Work done!")

    def get_pdfurl_from_webpage(self, url):
        html = urlopen(url)
        text = html.read()
        plaintext = text.decode('utf8')
        links = re.findall("href=[\"\'](.*?)[\"\']", plaintext)
        pdf = []
        for link in links:
            if ".pdf" in link and "coronavirus" in link:
                pdf.append(link)
        return pdf[0]

    def download_transform_pdf(self, url):
        # Geting datatime from name in url
        ind = url.index(".pdf")
        fecha = datetime.strptime(url[(ind-6):ind], "%d%m%y")
        # Downloading from url
        filename, _ = urlretrieve(url, "temp/minsareport.pdf")
        # Uploading pdf to bucket
        TODAY = str(datetime.now().date())
        destination = BUCKET_ROOT + "/report_minsa/" + TODAY + "/" + filename
        self.bucket.upload_blob(bucket_name=BUCKET_NAME,
                                source_file_name=filename,
                                destination_blob_name=destination)
        # Reading and transforming
        tables_raw = tabula.read_pdf(filename, pages=[3])
        alldata = tables_raw[0].sum(1).apply(
            lambda x: str(x).replace("  ", " "))
        assert len(alldata.loc[alldata.str.contains(
            "MUESTRAS TOTALES")]) > 0, "Table not found"
        # Extract data
        table = tabula.read_pdf(filename,
                                pages=[3],
                                guess=False,
                                area=(80, 10, 500, 1000))
        table = table[0].set_index("Región")
        table = table.applymap(lambda x: str(x).replace(" ", "").replace(
            ",", ".")).apply(pd.to_numeric, errors='ignore')
        assert "PCR" in table.columns.tolist(), "Doesn't contain PCR column"
        table.reset_index(inplace=True)
        table.drop(columns=["Región.1"], inplace=True)
        table["mypos"] = round((table["Total.1"]/table["Total"])*100, 1)
        table["fecha"] = fecha
        print(table.dtypes)
        table.rename(columns={"Región": "region",
                              "PCR": "PCR_total",
                              "PR": "PR_total",
                              "AG": "AG_total",
                              "PCR.1": "PCR_pos",
                              "PR.1": "PR_pos",
                              "AG.1": "AG_pos",
                              "Total.1": "Total_pos",
                              "% Positividad": "Positividad",
                              "mypos": "Positividad_verif"
                              }, inplace=True)
        if DB_positividad.objects.filter(fecha=fecha).exists():
            raise 'The record "fecha" already exist'
        print(table.head())
        table.region = table.region.apply(
            lambda x: normalizer_str(x).upper())
        index = table.loc[table.region == "TOTAL"].index[0]
        table.drop(index=[index], inplace=True)
        self.save_table(table, DB_positividad)

    def loading_dataset_pre(self):
        filename_local = "temp/positividad.csv"
        self.bucket.download_blob(bucket_name=BUCKET_NAME,
                                  source_blob_name="data_source/feed_rt.csv",
                                  destination_file_name=filename_local)
        data_pre = pd.read_csv(filename_local, sep=";")
        data_pre.REGION = data_pre.REGION.apply(
            lambda x: normalizer_str(x).upper())
        data_pre.drop(columns=["COUNTRY"], inplace=True)
        data_pre.rename(columns={"REGION": "region",
                                 "Fecha": "fecha",
                                 "cum_pos_pcr": "PCR_pos",
                                 "cum_pos_zero": "PR_pos",
                                 "cum_pos_ag": "AG_pos",
                                 "cum_pos_total": "Total_pos",
                                 "cum_total_muestras": "Total"}, inplace=True)
        cols = ["PCR_pos", "PR_pos", "AG_pos", "Total_pos", "Total"]
        data_pre[cols] = data_pre[cols].applymap(
            lambda x: str(x).replace(" ", "").replace("\u202f", "").replace(",", ".") if x == x else x)
        data_pre = data_pre.apply(pd.to_numeric, errors="ignore")
        print(data_pre.dtypes)
        data_pre = data_pre.fillna(0)
        self.save_table(data_pre, DB_positividad, init="yes")
