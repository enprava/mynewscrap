import copy
import math
import sys

import requests
import yaml
import pandas
from datetime import timedelta

busquedas = yaml.safe_load(open('busquedas.yaml'))
audios = []
data = {'id': [], 'termino': [], 'titular': [], 'fecha': [], 'duracion': [], 'enlace': []}

for busqueda, values in busquedas.items():
    print(busqueda)
    for value in values:
        response = requests.get(
            f'https://api.ondacero.es/cmsradios/v1/search/audios?siteId=2&offset=0&size=1000&text={busqueda}').json()
        n_busquedas = math.ceil(response['size'] / 1000)
        audios = copy.deepcopy(response['items'])
        for i in range(1, n_busquedas + 1):
            response = requests.get(
                f'https://api.ondacero.es/cmsradios/v1/search/audios?siteId=2&offset={i * 1000}&size=15&text={busqueda}').json()
            audios.extend(response['items'])

        for audio in audios:
            true_audio = audio['item']
            if 'duration' not in true_audio or 'shareUrl' not in true_audio:
                continue
            fecha = true_audio['date']
            if int(fecha[:4]) < 2018:
                continue
            id = true_audio['id']
            termino = busqueda
            titular = true_audio['title']
            seconds_duration = true_audio['duration']
            duracion = str(timedelta(seconds=seconds_duration))
            enlace = true_audio['shareUrl']

            data['id'].append(id)
            data['termino'].append(termino)
            data['titular'].append(titular)
            data['fecha'].append(fecha)
            data['duracion'].append(duracion)
            data['enlace'].append(enlace)

csv = pandas.DataFrame(data=data)
csv.to_csv('ondacero.csv', index=False)
