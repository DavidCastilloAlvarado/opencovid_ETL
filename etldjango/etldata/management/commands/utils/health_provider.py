import os
from etldjango.settings import KEY_MAPS_API
from google.cloud import storage
import requests
from tqdm import tqdm
import time
import pandas as pd
import numpy as np
import json
session = requests.Session()


class GetHealthFromGoogleMaps(object):
    KEY_API = KEY_MAPS_API
    fields = ['geometry__location', 'name', 'place_id',
              'rating', 'types', 'user_ratings_total']

    def __init__(self,):
        self.session = requests.Session()

    def get_location_by_name(self, places_name):
        locations = []
        for place_name in places_name:
            # time.sleep(.2)
            url = 'https://maps.googleapis.com/maps/api/place/findplacefromtext/json?input=' + \
                place_name+'&inputtype=textquery&fields=geometry&key='+self.KEY_API
            response = self.get_request_API(url)
            if response['status'] == 'OK':
                response = response['candidates'][0]
                geometry = response['geometry']
                locations.append(geometry['location'])
            else:
                locations.append({'lat': np.nan, 'lng': np.nan})
        return locations

    def get_request_API(self, url):
        try:
            response = self.session.get(url)
            response.raise_for_status()
            # access JSOn content
            #print("Entire JSON response")
            return response.json()

        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
            return {}
        except Exception as err:
            print(f'Other error occurred: {err}')
            return {}

    def get_oxi_places_from_points(self, point, radio=50000, place_extra=None):
        """
        tipo :  1 "venta",2 "recarga",3 "alquiler"
        place_extra: es el nombre de la locación en donde se quiere buscar, opcional.
        """
        tipo_negocio = ["venta", "recarga", "alquiler", ]

        latitude = point['lat']
        longitude = point['lng']
        radio = radio  # radio en metros
        places_all = []
        for tipo in tipo_negocio:
            query = tipo+'+de+oxigeno+medicinal+health'
            if place_extra:
                query = tipo+'+de+oxigeno+medicinal+health+en+'+place_extra
            url = 'https://maps.googleapis.com/maps/api/place/textsearch/json?query='+query + \
                '&language=es&location='+str(latitude)+','+str(longitude) + \
                '&radius='+str(radio)+'&key='+self.KEY_API
            #places = self.get_request_API(url)
            places = self.getting_places(url)
            places = self.extract_data_from_json(places)
            _ = [place.update({'negocio': tipo}) for place in places]
            places_all = places_all + places
        return places_all

    def get_drugstore_places_from_points(self, point, radio=50000):
        """
        Entrega los puntos en donde se encuentran las farmacias
        """
        queries = ['farmacia', 'boticas', 'mifarma',
                   'inkafarma', 'boticas y salud', 'arcangel']
        places = self.getting_all_drugstore_from_queries(queries, point, radio)
        places = self.extract_data_from_json(places)
        return places

    def getting_all_drugstore_from_queries(self, queries, point, radio):
        latitude = point['lat']
        longitude = point['lng']
        radio = radio  # radio en metros
        places = []
        for query in queries:
            url = 'https://maps.googleapis.com/maps/api/place/textsearch/json?query='+query + \
                '&language=es&location='+str(latitude)+','+str(longitude) + \
                '&radius='+str(radio)+'&key='+self.KEY_API
            #places = self.get_request_API(url)
            places = places + self.getting_places(url)
        return places

    def get_next_places_query(self, next_page_token):
        url = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json?pagetoken=' + \
            next_page_token+'&key='+self.KEY_API
        response = self.get_request_API(url)
        return response

    def getting_places(self, url):
        request = self.get_request_API(url)
        if request['status'] != 'OK':
            return []
        places_records = request['results']
        #print('next_page_token' in list(request.keys()))
        # print(list(request.keys()))
        # print(request['next_page_token'])
        while 'next_page_token' in list(request.keys()):
            # es necesario esperar un entretiempo entre solicitudes, de lo contrario, envía error
            time.sleep(2)
            request = self.get_next_places_query(request['next_page_token'])
            # print(request)
            places_records = places_records + request['results']
            # break
        return places_records

    def extract_data_from_json(self, places):
        records = []
        for place in places:
            record = dict()
            for field in self.fields:
                dato = place
                for subfield in field.split('__'):
                    try:
                        dato = dato[subfield]
                    except:
                        dato = None
                record[subfield] = dato
            details = self.place_details(record['place_id'])
            record.update(details)
            records.append(record)
        return records

    def place_details(self, place_id):
        fields = 'formatted_phone_number,website,formatted_address'  # price_level
        url = 'https://maps.googleapis.com/maps/api/place/details/json?place_id=' + \
            place_id+'&fields='+fields+'&key='+self.KEY_API
        details = self.get_request_API(url)
        if details['status'] == 'OK':
            details = details['result']
        else:
            details = {'formatted_phone_number': None,
                       'website': None,
                       'formatted_address': None, }
        return details
