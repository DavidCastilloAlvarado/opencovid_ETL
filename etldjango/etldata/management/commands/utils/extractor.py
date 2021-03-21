import urllib.request
import json
import PyPDF2
import pandas as pd
from tqdm import tqdm
from .urllibmod import urlretrieve


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
            return True
        except:
            return False

    def pdf_data_extractor(self, file_name):
        """
        Extrae información en formato texto de los documentos pdf
        """
        with open(r'downloads/{}.pdf'.format(file_name.split(".")[0]), 'rb') as fhandle:
            pdfReader = PyPDF2.PdfFileReader(fhandle)
            n_pages = pdfReader.getNumPages()

            json_file = {}
            txt_file = ""
            for pag in range(1, n_pages):
                pagehandle = pdfReader.getPage(pag)
                text = pagehandle.extractText()
                txt_file = txt_file + text + "\n"
                json_file[pag] = text

            with open('downloads/docs/{}.json'.format(file_name.split(".")[0]), 'w') as outfile:
                json.dump(json_file, outfile)
            with open('downloads/docs/{}.txt'.format(file_name.split(".")[0]), 'w') as outfile:
                json.dump(txt_file, outfile)
