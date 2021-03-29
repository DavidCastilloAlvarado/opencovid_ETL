import urllib.request
import json
import PyPDF2
import pandas as pd
from tqdm import tqdm
from .urllibmod import urlretrieve
from etldata.models import Logs_extractor


class Data_Extractor(object):
    def __init__(self, csv_urls="info_datos/datos_fuentes.csv", url=None, name=None, many=True):
        self.url = url
        self.name = name
        self.many = many
        if many:
            data = pd.read_csv(csv_urls)
            self.list_url = data.Download
            self.list_name = data.Nombre

    def extract_queue(self):
        if not self.many:
            return None
        self.results = [self.downloader(url, name)
                        for url, name in tqdm(zip(self.list_url, self.list_name), total=len(self.list_name))]

        if sum(self.results) == len(self.results):
            print("download_completed")
        else:
            print("download fail")
        return self.results

    def extract_one(self,):
        return self.downloader(self.url, self.name)

    def downloader(self, url, file_name):
        """
        Todas las descargas de almacenan en la carpeta descarga
        """
        try:
            #urllib.request.urlretrieve(url, 'downloads/{}'.format(file_name))
            urlretrieve(url, file_name)
            # if file_name.split(".")[-1] == "pdf":
            #    self.pdf_data_extractor(file_name)
            self.log_register({'e_name': file_name.split('/')[-1],
                               'url': url,
                               'status': 'ok',
                               'mode': 'download'})
            return True
        except:
            self.log_register({'e_name': file_name.split('/')[-1],
                               'url': url,
                               'status': 'fail',
                               'mode': 'download'})
            return False

    def log_register(self, data):
        Logs_extractor.objects.create(**data)
