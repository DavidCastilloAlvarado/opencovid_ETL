import os
from django.core.management.base import BaseCommand, CommandError
from datetime import datetime
from urllib.request import urlopen
from etldata.models import DB_daily
from tqdm import tqdm
import feedparser
from time import mktime
import time
import numpy as np
import re
import logging
logger = logging.getLogger('StackDriverHandler')


class Command(BaseCommand):
    RSS_URL = 'https://www.gob.pe/busquedas.rss?categoria[]=6-salud&contenido[]=noticias&desde=01-01-2021&sheet=1&sort_by=recent&tipo_noticia[]=3-comunicado'
    VACC_URL = 'https://www.gob.pe/institucion/pcm/campa%C3%B1as/3451-campana-nacional-de-vacunacion-contra-la-covid-19'
    help = 'This command extract data from Gob blogs'

    def print_shell(self, text):
        self.stdout.write(self.style.SUCCESS(text))

    def save_record(self, db):
        record = dict(
            fecha = self.fecha,
            url=self.url,
            total_muestras = self.total_muestras,
            total_neg= self.total_neg,
            total_pos= self.total_pos,
            dia_muestras = self.dia_muestras,
            dia_pos = self.dia_pos,
            hosp_zc_total = self.hosp_zc_total,
            hosp_zc_uci = self. hosp_zc_uci,
            recuperados = self.recuperados,
            fallecidos = self.fallecidos,
            vacc_1d = self. vacc_1d,
            vacc_2d = self.vacc_2d,
        )
        valid = [var[1] != 0 for var in record.items()]
        valid = all(valid)
        if not db.objects.filter(url=self.url,).exists() and valid:
            records = [record]
            print(records)
            records = [db(**record) for record in tqdm(records)]
            _ = db.objects.bulk_create(records)
            self.print_shell('Record saved')
        else:
            self.print_shell('Record not saved')
        #_ = db.objects.all().delete()
        #_ = db.objects.bulk_create(records)

    def handle(self, *args, **options):
        rss_result = self.capture_url_from_rss(DB_daily,mode='many')
        for feed in rss_result:
            doc = self.extract_rawdata_from_feed(feed)
            self.extract_metrics_from_doc_feed(doc)
            self.capture_data_from_gob_vacc()
            self.save_record(DB_daily)
        self.print_shell("Work Done!")

    def capture_url_from_rss(self,db, mode='single'):
        NewsFeed = feedparser.parse(self.RSS_URL)
        text= 'Minsa: Casos confirmados por coronavirus COVID-19'
        feed_list=[]
        for entry in NewsFeed.entries:
            if text in entry['title']:
                date = datetime.fromtimestamp(mktime(entry['published_parsed']))
                link = entry['link']
                feed = dict(
                    link=link,
                    date=date.date()
                )
                if not db.objects.filter(url=feed['link'],).exists():
                    feed_list.append(feed)
        #print(feed_list)
        if mode=='single':
            return [feed_list[0]]
        return feed_list

    def extract_rawdata_from_feed(self,feed):
        self.fecha = feed['date']
        self.url = feed['link']
        html = urlopen(feed['link'])
        text = html.read()
        plaintext = text.decode('utf8')
        contents = re.findall("data-contents=[\"\'](.*?)[\"\']", plaintext)
        doc = []
        for content in contents:
            doc.append(content)
        return doc[0]

    def extract_rawdata_from_vacc_gob(self):
        html = urlopen(self.VACC_URL)
        text = html.read()
        plaintext = text.decode('utf8')
        return plaintext

    def extract_metrics_from_doc_feed(self, doc):
        #print(doc)
        self.total_muestras, last_ix = self.number_extractor(doc,'procesado muestras para','personas')
        self.total_neg, last_ix = self.number_extractor(doc, ', obteniéndose,', 'casos confirmados', last_ix)
        self.total_pos, last_ix = self.number_extractor(doc, 'confirmados y', 'negativos.', last_ix)
        self.dia_muestras, last_ix = self.number_extractor(doc, 'registraron los resultados de', 'personas muestreadas', last_ix)
        dia_pos, last_ix = self.number_extractor(doc, 'de los cuales', 'fueron casos sintomáticos', last_ix)
        parcial_pos, last_ix = self.number_extractor(doc, 'registraron parcialmente, además, los resultados de', 'casos confirmados por', last_ix)
        self.dia_pos = dia_pos + parcial_pos
        self.hosp_zc_total, last_ix = self.number_extractor(doc, '4. A la fecha, se tienen', 'pacientes hospitalizados', last_ix)
        self.hosp_zc_uci, last_ix = self.number_extractor(doc, 'de los cuales,', 'se encuentran en UCI', last_ix)
        self.recuperados, last_ix = self.number_extractor(doc, 'Del total de casos confirmados, a la fecha,', 'personas cumplieron su período', last_ix)
        self.fallecidos, last_ix = self.number_extractor(doc, 'ha producido el fallecimiento de', 'ciudadanos en el país.', last_ix)

    @staticmethod
    def number_extractor(texto, str_ini, str_end, ini_index=0):
        try:
            index1 = texto.index(str_ini,ini_index) + len(str_ini)
            index2 = texto.index(str_end, index1)
            number_str = texto[int(index1): int(index2)].strip().replace(' ','').replace(',','')
            number = float(number_str)
            print(number)
            return number,index2
        except :
            logger.critical('Error - Daily_report: '+str_ini+' | '+ str_end)
            return 0,0

    def capture_data_from_gob_vacc(self):
        data = self.extract_rawdata_from_vacc_gob()
        self.vacc_1d, last_ix = self.number_extractor(data, 'Total: 1ra dosis</div><div class="font-bold text-3xl leading-none">','</div><div' )
        self.vacc_2d, last_ix = self.number_extractor(data, 'Total: 2da dosis</div><div class="font-bold text-3xl leading-none">','</div><div', last_ix )