import scrapy
import json
from etl import get_databse, insert_many

'''
    Cria um arquivo json contendo as marcas inseridas por parâmetro
'''
def write_results_file(brands):
    ordered_brands = sorted(brands, key=lambda d: d['name'].lower())
    jsonstring = json.dumps(ordered_brands)
    jsonfile = open('brands.json', 'w')

    jsonfile.write(jsonstring)
    jsonfile.close()

'''
    Cria uma conexão com o mongodb, abre o arquivo json contendo as marcas e as envia para a coleção 'brands'
'''
def send_to_database():
    cluster_name = 'cluster0.mwrqoei.mongodb.net'
    username = 'giuliabrocchi'
    password = 'abcd1234'
    database = 'competitors'

    database = get_databse(cluster_name, username, password, database)

    with open('brands.json', 'r') as brands_file:
        brands = json.load(brands_file)
        insert_many(database, 'brands', brands)

'''
    Retorna uma lista contendo as urls com filtro de letras (A até Z)
'''
def urls_brands():
    base_url = 'https://www.rankingthebrands.com/The-Brands-and-their-Rankings.aspx?catFilter=0&nameFilter='
    alfabeto = 'ABCDEFGHIJKLMNOPQRSTUVXWYZ'
    urls = list()
    for l in alfabeto:
        urls.append(base_url + l)
    return urls

class BlogSpider(scrapy.Spider):
    name = 'blogspider'
    start_urls = urls_brands()
    brands = list()

    def parse(self, response):
        for name in response.css('.rankingName'):
            brand = name.css('::text').get()
            self.brands.append({'name': brand})
            yield {'name': brand}

    def close(self, reason):
        write_results_file(self.brands)
        send_to_database()
