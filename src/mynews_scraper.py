import asyncio
import concurrent
import json
import math
import os.path
import sys
import time
from datetime import datetime, timedelta

import aiohttp
import pandas as pd

import logging

import requests
from dateutil.relativedelta import relativedelta
from selenium.webdriver.support.wait import WebDriverWait
from seleniumwire import webdriver
from seleniumwire.utils import decode
from selenium.webdriver import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.relative_locator import locate_with
from selenium.webdriver.support.select import Select
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.firefox.options import Options
from tqdm import tqdm

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)
logging.getLogger("seleniumwire").setLevel(logging.WARNING)


class MyNewsScrapper:
    def __init__(self, config, medios_mapping, terminos_mapping):
        self.config = config
        self.medios_mapping = medios_mapping
        self.terminos_mapping = terminos_mapping

        self.logger = logging.getLogger(f'{self.__class__.__name__}')
        self.logger.info('Inicialización <MyNewsScrapper> (L)')

        self.datos_columnas = ['id', 'termino', 'fecha', 'medio', 'titular', 'enlace_local', 'enlace_externo',
                               'tamaño_bytes', 'status']
        self.almacen = []

        self.headers = {}

        self.fecha_comienzo_busqueda = self.config['fecha_inicio_busqueda']
        self.fecha_fin_busqueda = self.config['fecha_fin_busqueda']

        self.fecha_puntero = self.fecha_comienzo_busqueda
        self.termino_busqueda_puntero = None

        self.inicializar_almacen()

        self.logger.info('Inicialización finalizada')

    def inicializar_almacen(self):
        self.logger.info('Inicialización almacen con directorio: %s', self.config['directorio_almacen'])

        if not os.path.exists(self.config['directorio_almacen']):
            self.almacen = pd.DataFrame(columns=self.datos_columnas)
        else:
            self.almacen = pd.read_csv(self.config['directorio_almacen'])
        self.logger.info('almacen inicializado con %d entradas', len(self.almacen))

    def entrar_mynews(self):
        self.logger.info('Autenticando en MyNews con un nuevo navegador')
        options = Options()
        if self.config['headless']:
            options.headless = True
        driver = webdriver.Firefox(options=options, executable_path='geckodriver')
        driver.get('https://us--mynews--es.us.debiblio.com/hu/')
        driver.find_element(By.ID, 'edit-name').send_keys(self.config['usuario'])
        driver.find_element(By.ID, 'edit-pass').send_keys(self.config['contraseña'] + Keys.ENTER)
        WebDriverWait(driver, 30).until(
            expected_conditions.url_to_be('https://us--mynews--es.us.debiblio.com/hu/'))

        self.driver = driver
        for peticion in reversed(self.driver.requests):
            if peticion.url == 'https://us--mynews--es.us.debiblio.com/hu/':
                self.headers = peticion.headers
                break

        self.logger.info('Navegador preparado en MyNews')

    def comenzar_extraccion(self):

        for termino_busqueda in self.config['terminos_busqueda']:
            self.logger.info('Inicia proceso de extracción para el termino: %s', termino_busqueda)

            while self.fecha_puntero < self.fecha_fin_busqueda:
                if termino_busqueda != self.termino_busqueda_puntero:
                    # Renovamos la autentificación y el navegador por cada término de busqueda.
                    self.logger = logging.getLogger(f'{self.__class__.__name__} [{termino_busqueda}]')
                    if self.termino_busqueda_puntero != None:  # Cerramos el navegador antiguo, excepto en la 1º iteracion
                        self.driver.close()
                    self.entrar_mynews()
                    self.termino_busqueda_puntero = termino_busqueda
                    fecha_inicio_busqueda_actual = self.fecha_comienzo_busqueda
                else:
                    fecha_inicio_busqueda_actual = self.fecha_puntero

                if self.config['periodicidad'] == 'mensual':
                    fecha_fin_busqueda_actual = self.fecha_puntero + relativedelta(
                        months=self.config['amplitud_busqueda'])
                else:  # Intervalo completo, sin contemplar la periodicidad.
                    fecha_fin_busqueda_actual = self.fecha_fin_busqueda

                if fecha_fin_busqueda_actual > self.fecha_fin_busqueda:
                    fecha_fin_busqueda_actual = self.fecha_fin_busqueda

                self.fecha_puntero = fecha_fin_busqueda_actual
                try:
                    self.realizar_busqueda(termino_busqueda, fecha_inicio_busqueda_actual, fecha_fin_busqueda_actual)
                except:
                    self.logger.warning('Ha fallado algo durante el proceso de busqueda, reintentando...')
                    self.realizar_busqueda(termino_busqueda, fecha_inicio_busqueda_actual, fecha_fin_busqueda_actual)

            self.fecha_puntero = self.fecha_comienzo_busqueda

    def realizar_busqueda(self, termino_busqueda, fecha_inicio, fecha_fin):
        self.logger.info('Buscando entre las fechas %s - %s', fecha_inicio, fecha_fin)
        self.driver.get('https://us--mynews--es.us.debiblio.com/hu/busqueda/profesional/')

        formulario_busqueda = self.driver.find_element(By.ID, 'busqueda_booleana')
        for termino in self.terminos_mapping[termino_busqueda]:
            formulario_busqueda.send_keys(f"\"{termino}\"OR")
        formulario_busqueda.send_keys(Keys.BACKSPACE)
        formulario_busqueda.send_keys(Keys.BACKSPACE)

        self.driver.find_element(By.ID, 'selectorDatesButton').click()
        self.driver.find_element(By.CLASS_NAME, 'sel_date_2.cell.small-10.no-padding-left.pointer').click()

        dia_inicio, mes_inicio, año_inicio = fecha_inicio.day, fecha_inicio.month, fecha_inicio.year
        dia_fin, mes_fin, año_fin = fecha_fin.day, fecha_fin.month, fecha_fin.year

        Select(self.driver.find_element(By.ID, 'data_time_1_day')).select_by_value(str(dia_inicio))
        Select(self.driver.find_element(By.ID, 'data_time_1_month')).select_by_value(str(mes_inicio))
        Select(self.driver.find_element(By.ID, 'data_time_1_year')).select_by_value(str(año_inicio))

        Select(self.driver.find_element(By.ID, 'data_time_2_day')).select_by_value(str(dia_fin))
        Select(self.driver.find_element(By.ID, 'data_time_2_month')).select_by_value(str(mes_fin))
        Select(self.driver.find_element(By.ID, 'data_time_2_year')).select_by_value(str(año_fin))

        self.driver.find_element(locate_with(By.TAG_NAME, 'div').below({By.ID: 'datePicker-container'})).click()

        self.driver.execute_script(f"document.getElementById('source_idnum').value={self.config['filtro_relevancia']}")

        self.driver.find_element(By.ID, 'selectorMitjans').click()
        self.driver.find_element(By.ID, 'selector_check_all').click()  # Deseleccionamos todos los medios

        for medio in self.config['medios']:
            self.driver.find_element(By.ID, self.medios_mapping[medio]).click()

        self.driver.find_element(By.ID, 'enviarPublicacions').click()

        # Seleccionar categoria de deportes TODO: Esto debería ser otro mapa.
        Select(self.driver.find_element(By.CLASS_NAME, 'cell.small-12.medium-8.no-padding-mobil')
               .find_elements(By.CLASS_NAME, 'boxRectangle')[1]
               .find_element(By.CLASS_NAME, 'grid-x.grid-padding-x.align-center')
               .find_elements(By.CLASS_NAME, 'cell.small-7.medium-4.subseccio')[1]
               .find_element(By.TAG_NAME, 'select')).select_by_value('2')

        self.driver.find_element(By.ID, 'search_button').click()

        try:
            WebDriverWait(self.driver, 30).until(
                expected_conditions.presence_of_element_located((By.ID, 'llistaNoticies')))


        except Exception as e:
            self.logger.warning("La busqueda de noticias ha fallado. Params: {} {} {}", termino_busqueda, fecha_inicio,
                                fecha_fin)
            return None

        self.logger.info('Busqueda terminada')

        peticiones = self.montar_peticiones_paginacion()
        if not len(peticiones):
            self.logger.warning('No se han encontrado noticias con el termino de busqueda')
            return None
        noticias = self.traer_noticias(peticiones)
        self.almacenar_informacion_noticias_sin_descargar(noticias)
        # self.descargar_noticias(noticias)

    def montar_peticiones_paginacion(self):
        self.logger.info('Montando peticiones')

        while True:
            time.sleep(1)
            try:
                for peticion in reversed(self.driver.requests):
                    url = peticion.url
                    headers = peticion.headers
                    if 'noticies' in url:
                        bytes_body = decode(peticion.response.body,
                                            peticion.response.headers.get('Content-Encoding', 'identity'))
                        str_body = bytes_body.decode('utf-8')
                        body = json.loads(str_body)
                        break  # Nos quedamos con la petición que contiene  las 'noticies'  más reciente
            except:
                continue
            break

        begin_hash_index = url.find('=') + 1
        end_hash_index = url.find('&')
        hash_key = url[begin_hash_index:end_hash_index]
        total_noticies = body['totalHits']
        self.logger.info('Noticias Totales (Supuestas por HTTP): %d', total_noticies)

        tamaño_pagina = self.config['tamaño_pagina']
        num_paginas = math.ceil(total_noticies / tamaño_pagina)

        relevancia = self.config['filtro_relevancia']

        peticiones = [requests.Request('get',
                                       f'https://us--mynews--es.us.debiblio.com/hu/noticies/page/{i + 1}/?'
                                       f'hash_keys={hash_key}'
                                       f'&order=coincidencia'
                                       f'&desc=true'
                                       f'&rellevancia={relevancia}'
                                       f'&resultsPage={tamaño_pagina}&tipusResultats=',
                                       headers=headers)
                      for i in range(0, num_paginas)]

        self.logger.info('Número de peticiones/Páginas encontradas: %d ', num_paginas)

        return peticiones

    def traer_noticias(self, peticiones):
        self.logger.info('Enviando las peticiones a Mynews')
        session = requests.session()
        noticies = []
        for indice_pagina, peticion in enumerate(peticiones):
            self.logger.info('Página %d en proceso', indice_pagina + 1)
            try:
                respuesta = session.send(peticion.prepare())
                respuesta.raise_for_status()
                respuesta = respuesta.json()
                noticies = noticies + respuesta['noticies']
                self.logger.info('Alcanzadas %d noticias en la página %s', len(respuesta['noticies']),
                                 indice_pagina + 1)
                if len(respuesta['noticies']) < self.config['tamaño_pagina']:
                    self.logger.info('Se ha detectado que esta es la última página.')
                    break
            except:
                self.logger.warning('Ha fallado la petición: %s, es posible que no haya más paginas', peticion.url)
                return noticies

        session.close()
        return noticies

    def almacenar_informacion_noticias_sin_descargar(self, noticias):
        self.logger.info('Almacenando información en el fichero CSV ')
        noticias_candidatas = len(noticias)
        self.logger.info('%d noticias candidatas', noticias_candidatas)
        noticias_df = pd.DataFrame(noticias)
        noticias_df['termino'] = self.termino_busqueda_puntero
        noticias_df['status'] = None
        directorios_internos = None
        noticias_df['url_interna'] = None
        noticias_df['tamaño_fichero'] = None
        noticias_df = noticias_df[
            ['IdDocument', 'termino', 'date', 'Newspaper', 'Title', 'Page', 'url_interna', 'tamaño_fichero', 'status']]
        noticias_df.columns = self.datos_columnas

        tamaño_almacen_pre = len(self.almacen)
        self.almacen = pd.concat([self.almacen, noticias_df])

        self.almacen.drop_duplicates(['titular', 'fecha', 'medio'], keep='first', inplace=True)
        tamaño_almacen_post = len(self.almacen)
        noticias_insertadas = tamaño_almacen_post - tamaño_almacen_pre
        noticias_duplicadas = noticias_candidatas - noticias_insertadas

        self.guardar_almacen()
        self.logger.info("Se han insertado %s noticias habiendo %s duplicadas", noticias_insertadas,
                         noticias_duplicadas)

    def descargar_noticias(self):
        noticias_por_descargar = self.almacen[self.almacen['status'].isna()]
        self.logger.info('Descargando entradas %d', len(noticias_por_descargar))

        if not len(noticias_por_descargar):
            self.logger.info('No hay noticias que extraer')
            return 1

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.config['hilos_descarga']) as executor:
            try:
                list(tqdm(executor.map(self.descargar_noticia, noticias_por_descargar.itertuples(index=False)),
                          total=len(noticias_por_descargar)))
            except Exception as e:
                self.logger.error('Ha fallado el proceso de descarga %s', e)

        self.guardar_almacen()

        return 1

    def descargar_noticia(self, noticia):
        id = getattr(noticia, 'id')
        termino = getattr(noticia, 'termino')
        medio = getattr(noticia, 'medio')
        fecha = datetime.strptime(getattr(noticia, 'fecha'), '%d/%m/%Y')

        directorio = os.path.join(self.config['directorio_raiz_noticias_descargadas'], termino, str(fecha.year),
                                  medio)
        os.makedirs(directorio, exist_ok=True)
        directorio_fichero = os.path.join(directorio, id + '.pdf')
        if not os.path.exists(directorio_fichero):
            url = f'https://us--mynews--es.us.debiblio.com/hu/noticies/?idDocument={id}&tipus=pdf'
            try:
                response_pdf = requests.get(url=url, headers=self.headers)
                response_pdf.raise_for_status()
            except Exception as e:
                self.logger.warning('%s falló la autentificación: %s', id, e)
                raise e

            try:
                fichero = open(directorio_fichero, 'wb')
                fichero.write(response_pdf.content)
                fichero.close()
                # tamaño_fichero = len(response_pdf.content)
                # df = self.almacen
                # df.loc[df['id'] == id,'status'] = 0
                # print(df)
                # print(self.almacen.loc[self.almacen['id' == id], 'status'])
                # self.almacen.loc[self.almacen.loc['id' == id]]['status'] = 'Extraido'
                # self.almacen.loc[self.almacen.loc['id' == id]]['tamaño_bytes'] = tamaño_fichero
                # self.almacen.loc[self.almacen.loc['id' == id]]['enlace_local'] = directorio_fichero

                self.guardar_almacen()
            except Exception as e:
                self.logger.warning('Error al guardar el fichero: %s - %s', id,e)
                print(self.almacen.loc[self.almacen['id' == id], 'status'])
                raise e

    def guardar_almacen(self):
        self.almacen.to_csv(self.config['directorio_almacen'], index=False)
