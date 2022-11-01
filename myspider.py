import scrapy
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor, defer
import base64
import requests
import json
from etl import get_databse, insert_many

brands = list()
brands_urls = list()
base_url = 'https://www.rankingthebrands.com/'

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
	Cria uma conexão com o mongodb, abre o arquivo json contendo as marcas e as envia para
	a coleção 'brands'
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
	url = base_url + 'The-Brands-and-their-Rankings.aspx?catFilter=0&nameFilter='
	alfabeto = 'ABCDEFGHIJKLMNOPQRSTUVXWYZ'
	urls = list()
	for l in alfabeto:
		urls.append(url + l)
	return urls

'''
	Classe crawler para extrair os nomes de todas as empresas
'''
class BlogSpider(scrapy.Spider):
	name = 'blogspider'
	start_urls = urls_brands()

	def parse(self, response):
		for element in response.css('.list'):
			brands_urls.append(base_url + element.css('::attr(href)').get())
			brand = element.css('span[class=rankingName] ::text').get()
			brands.append({'name': brand})
			yield {'name': brand}

'''
	Crawler para extrair informações extras de uma empresa, criar um arquivo json com as
	informações encontradas e enviar para o bando de dados MongoDB
'''
class BrandSpider(scrapy.Spider):
	name = 'brandspider'
	gbin_selector = 'span[id=ctl00_mainContent_LBLGBIN] ::text'
	website_selector = 'span[id=ctl00_mainContent_LBBrandWebsite] ::text'
	country_selector = 'span[id=ctl00_mainContent_LBCountryOfOrigin] ::text'
	industry_selector = 'span[id=ctl00_mainContent_LBBrandIndustry] span a ::text'
	image_selector = 'span[id=ctl00_mainContent_LBBrandLogo] img ::attr(src)'

	def start_requests(self):
		for i in range(0, 10):
			url = brands_urls[i]
			yield scrapy.Request(url, self.parse)

	def parse(self, response):
		for info in response.css('.branddetails-Left'):
			name = info.css('span[id=ctl00_mainContent_LBBrandName] ::text').get()
			for brand in brands:
				if brand['name'] == name:
					image_url = base_url + info.css(self.image_selector).get()
					brand['image'] = base64.b64encode(requests.get(image_url).content).decode('utf-8')
					brand['GBIN'] = info.css(self.gbin_selector).get()
					brand['website'] = info.css(self.website_selector).get()
					brand['country'] = info.css(self.country_selector).get()
					brand['industry'] = info.css(self.industry_selector).get()
			yield {'info': info}

	def close(self, reason):
		write_results_file(brands)
		send_to_database()

settings = get_project_settings()
configure_logging(settings)
runner = CrawlerRunner(settings)

@defer.inlineCallbacks
def crawl():
	yield runner.crawl(BlogSpider)
	yield runner.crawl(BrandSpider)
	reactor.stop()

crawl()
reactor.run()
