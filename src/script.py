import time

import requests
import yaml
from selenium.webdriver.support.wait import WebDriverWait
from seleniumwire import webdriver
from selenium.webdriver import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.relative_locator import locate_with
from selenium.webdriver.support.select import Select
from selenium.webdriver.support import expected_conditions
import pandas
import os

if not os.path.exists('noticias'):
    os.mkdir('noticias')

csv = pandas.read_csv('busquedas.csv')

dictionary = {'ID': [], 'Termino': [], 'Fecha': [], 'Medio': [], 'Titular': [], 'Enlace local': [],
              'Enlace intenet': [], 'Tam': []}
config = yaml.safe_load(open('config/config.yaml', 'r', encoding='utf-8'))
medios_mapping = yaml.safe_load(open('config/medios_mapping.yaml', 'r', encoding='utf-8'))
terminos_mapping = yaml.safe_load(open('config/terminos_mapping.yaml', 'r', encoding='utf-8'))
termino_anterior = None
driver = None

for row in csv.iterrows():
    termino_actual = row[1]["search_term"]
    if not termino_actual == termino_anterior:
        if driver:
            driver.quit()
        driver = webdriver.Firefox()
        driver.get('https://us--mynews--es.us.debiblio.com/hu/')
        driver.find_element(By.ID, 'edit-name').send_keys('')
        driver.find_element(By.ID, 'edit-pass').send_keys('' + Keys.ENTER)
        WebDriverWait(driver, 10).until(
            expected_conditions.url_to_be('https://us--mynews--es.us.debiblio.com/hu/'))

    driver.get('https://us--mynews--es.us.debiblio.com/hu/busqueda/profesional/')
    driver.find_element(By.ID, 'busqueda_booleana').send_keys(terminos_mapping[row[1]["search_term"]])
    driver.find_element(By.ID, 'selectorDatesButton').click()

    driver.find_element(By.CLASS_NAME, 'sel_date_2.cell.small-10.no-padding-left.pointer').click()

    init_date = row[1]['begin_date']
    day_1 = Select(driver.find_element(By.ID, 'data_time_1_day')).select_by_value(str(int(init_date[-2:])))
    month_1 = Select(driver.find_element(By.ID, 'data_time_1_month')).select_by_value(str(int(init_date[5:7])))
    year_1 = Select(driver.find_element(By.ID, 'data_time_1_year')).select_by_value(init_date[:4])

    finish_date = row[1]['end_date']
    day_2 = Select(driver.find_element(By.ID, 'data_time_2_day')).select_by_value(str(int(finish_date[-2:])))
    month_2 = Select(driver.find_element(By.ID, 'data_time_2_month')).select_by_value(str(int(finish_date[5:7])))
    year_2 = Select(driver.find_element(By.ID, 'data_time_2_year')).select_by_value(finish_date[:4])

    button = locate_with(By.TAG_NAME, 'div').below({By.ID: 'datePicker-container'})
    driver.find_element(button).click()

    driver.execute_script("document.getElementById('source_idnum').value=60")

    driver.find_element(By.ID, 'selectorMitjans').click()
    driver.find_element(By.ID, 'selector_check_all').click()  # Deseleccionamos todos los medios
    for medio in config['medios']:
        driver.find_element(By.ID, medios_mapping[medio]).click()

    driver.find_element(By.ID, 'enviarPublicacions').click()

    Select(driver.find_element(By.CLASS_NAME, 'cell.small-12.medium-8.no-padding-mobil')
           .find_elements(By.CLASS_NAME, 'boxRectangle')[1]
           .find_element(By.CLASS_NAME, 'grid-x.grid-padding-x.align-center')
           .find_elements(By.CLASS_NAME, 'cell.small-7.medium-4.subseccio')[1]
           .find_element(By.TAG_NAME, 'select')).select_by_value('2')

    driver.find_element(By.ID, 'search_button').click()
    try:
        WebDriverWait(driver, 30).until(expected_conditions.presence_of_element_located((By.ID, 'llistaNoticies')))
    except Exception as e:
        pass
    time.sleep(1)
    for i in reversed(range(len(driver.requests))):
        if 'noticies' in driver.requests[i].url:
            url = driver.requests[i].url
            headers = driver.requests[i].headers
            break
    url = url.replace('order=data', 'order=coincidencia')
    url = url.replace('resultsPage=25', 'resultsPage=1000')
    response = requests.get(url=url, headers=headers).json()
    url_format = 'https://us--mynews--es.us.debiblio.com/hu/noticies/?idDocument={}&tipus=pdf'
    path = f'noticias/{row[1]["search_term"]}'
    if not os.path.exists(path):
        os.mkdir(path)
    for noticie in response['noticies']:
        titular = noticie['Title']
        date = noticie['date']
        medio = noticie['Newspaper']
        if titular in dictionary['Titular']:
            indexes = [index for index, v in enumerate(dictionary['Titular']) if v == titular]
            boolean = False
            for index in indexes:
                if date == dictionary['Fecha'][index] and medio == dictionary['Medio'][index]:
                    boolean = True
            if boolean:
                continue
        year = noticie['date'][-4:]
        if not os.path.exists(f'{path}/{year}'):
            os.mkdir(f'{path}/{year}')
        if not os.path.exists(f'{path}/{year}/{medio}'):
            os.mkdir(f'{path}/{year}/{medio}')
        id_document = noticie['IdDocument']
        try:
            response_pdf = None
            i = 1
            while not response_pdf:
                print(f'{i}: {id_document}\n')
                response_pdf = requests.get(url=url_format.format(id_document), headers=headers)
                i += 1
                response_pdf.raise_for_status()
        except Exception as e:
            print('\n', e, '\n', id_document, '\n')
            continue
        tam = len(response_pdf.content)
        if tam in dictionary['Tam']:
            indexes = [index for index, v in enumerate(dictionary['Tam']) if v == tam]
            for index in indexes:
                boolean = False
                if date == dictionary['Fecha'][index] and medio == dictionary['Medio'][index]:
                    boolean = True
            if boolean:
                continue
        pdf = open(f'{path}/{year}/{medio}/{id_document}.pdf', 'wb')
        pdf.write(response_pdf.content)
        pdf.close()
        dictionary['ID'].append(id_document)
        dictionary['Titular'].append(titular)
        dictionary['Fecha'].append(date)
        dictionary['Termino'].append(row[1]['search_term'])
        dictionary['Enlace local'].append(f'{path}/{year}/{medio}/{id_document}.pdf')
        dictionary['Medio'].append(medio)
        url_noticie = noticie['Page'] if 'http' in noticie['Page'] else None
        dictionary['Enlace intenet'].append(url_noticie)
        dictionary['Tam'].append(tam)
    noticias = pandas.DataFrame(data=dictionary)
    noticias.to_csv('tabla_access.csv', index=False, sep=';')
    termino_anterior = termino_actual

noticias = pandas.DataFrame(data=dictionary)
noticias.to_csv('tabla_access.csv', index=False, sep=';')
