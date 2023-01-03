# Aluna: Giulia Brocchi

## Projeto
Crawler para extrair informações de marcas

## Setup
```
python myspider.py
```

## Observações
Como há mais de 8000 empresas cadastradas no site, buscar as informações sobre cada empresa em seu URL é muito demorado.
Portanto, para facilitar a busca, o programa está limitado para buscar mais informações somente com as 10 primeiras empresas encontradas.
Para buscar as informações completas de todas as empresas, troque o código das linhas 64 e 65:
```
for i in range(0, 10):
  url = brands_urls[i]
  yield scrapy.Request(url, self.parse)
```
pelo seguinte código:
```
for url in brands_urls:
  yield scrapy.Request(url, self.parse)
```
