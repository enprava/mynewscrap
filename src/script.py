import requests
from seleniumwire import webdriver
from selenium.webdriver import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.relative_locator import locate_with
from selenium.webdriver.support.select import Select
import time
import pandas
import os

csv = pandas.read_csv('busquedas.csv')

driver = webdriver.Firefox()

driver.get('https://us--mynews--es.us.debiblio.com/hu/')
driver.find_element(By.ID, 'edit-name').send_keys('')
driver.find_element(By.ID, 'edit-pass').send_keys('' + Keys.ENTER)
time.sleep(10)
dictionary = {'ID': [], 'Termino': [], 'Fecha': [], 'Medio': [], 'Titular': [], 'Enlace local': [],
              'Enlace intenet': []}
for row in csv.iterrows():
    driver.get('https://us--mynews--es.us.debiblio.com/hu/busqueda/profesional/')
    # driver.find_element(By.ID, 'busqueda_booleana').send_keys(row[1]['search_term'].replace(' ', ' AND '))
    driver.find_element(By.ID, 'busqueda_booleana').send_keys(row[1]['search_term'])
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
    driver.find_element(By.ID, 'selector_check_all').click()
    driver.find_element(By.ID, 'selector_ref_379').click()  # Marca
    driver.find_element(By.ID, 'selector_ref_170').click()
    driver.find_element(By.ID, 'selector_ref_163').click()
    driver.find_element(By.ID, 'enviarPublicacions').click()

    Select(driver.find_element(By.CLASS_NAME, 'cell.small-12.medium-8.no-padding-mobil')
           .find_elements(By.CLASS_NAME, 'boxRectangle')[1]
           .find_element(By.CLASS_NAME, 'grid-x.grid-padding-x.align-center')
           .find_elements(By.CLASS_NAME, 'cell.small-7.medium-4.subseccio')[1]
           .find_element(By.TAG_NAME, 'select')).select_by_value('2')

    driver.find_element(By.ID, 'search_button').click()
    time.sleep(15)
    url, headers = None, None
    for i in reversed(range(len(driver.requests))):
        if 'noticies' in driver.requests[i].url:
            url = driver.requests[i].url
            headers = driver.requests[i].headers
            break
    url = url.replace('order=data', 'order=coincidencia')
    url = url.replace('resultsPage=25', 'resultsPage=400')
    response = requests.get(url=url, headers=headers).json()
    url_format = 'https://us--mynews--es.us.debiblio.com/hu/noticies/?idDocument={}&tipus=pdf'
    path = f'noticias/{row[1]["search_term"]}'
    if not os.path.exists(path):
        os.mkdir(path)
    for noticie in response['noticies']:
        year = noticie['date'][:-4]
        if not os.path.exists(f'{path}/{year}'):
            os.mkdir(f'{path}/{year}')
        medio = noticie['Newspaper']
        if not os.path.exists(f'{path}/{year}/{medio}'):
            os.mkdir(f'{path}/{year}{medio}')
        id_document = noticie['IdDocument']
        try:
            response_pdf = None
            i = 0
            while not response_pdf:
                print(f'{i}: {id_document}')
                response_pdf = requests.get(url=url_format.format(id_document), headers=headers)
        except Exception as e:
            print(e)
        pdf = open(f'{path}/{medio}/{id_document}.pdf', 'wb')
        pdf.write(response_pdf.content)
        pdf.close()
        dictionary['ID'].append(id_document)
        dictionary['Titular'].append(noticie['Title'])
        dictionary['Fecha'].append(noticie['date'])
        dictionary['Termino'].append(row[1]['search_term'])
        dictionary['Enlace local'].append(f'{path}/{id_document}.pdf')
        dictionary['Medio'].append(medio)
        url_noticie = noticie['Page'] if 'http' in noticie['Page'] else None
        dictionary['Enlace intenet'].append(url_noticie)
    noticias = pandas.DataFrame(data=dictionary)
    noticias.to_csv('tabla_access.csv', index=False, sep=';')

noticias = pandas.DataFrame(data=dictionary)
noticias.to_csv('tabla_access.csv', index=False, sep=';')
driver.quit()
