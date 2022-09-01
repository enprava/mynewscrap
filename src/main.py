import yaml
from mynews_scraper import MyNewsScrapper

if __name__ == '__main__':
    config = yaml.safe_load(open('config/config.yaml', 'r', encoding='utf-8'))
    medios_mapping = yaml.safe_load(open('config/medios_mapping.yaml', 'r', encoding='utf-8'))
    terminos_mapping = yaml.safe_load(open('config/terminos_mapping.yaml', 'r', encoding='utf-8'))

    scraper = MyNewsScrapper(config, medios_mapping, terminos_mapping)
    driver = scraper.entrar_mynews()
    scraper.comenzar_extraccion()
    # scraper.descargar_noticias()
    # scraper.guardar_almacen()

    # taskkill /IM firefox.exe /F
