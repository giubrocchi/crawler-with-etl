from pymongo import MongoClient
from pymongo.database import Database
from pymongo.cursor import Cursor
from pymongo.results import DeleteResult

'''
  Conecta em um bd mongo estando em serviço mongo atlas (padrão mongodb+srv) e retorna o banco desejado
'''
def get_databse(cluster_name: str, username: str, password: str, database: str) -> Database:
  client = MongoClient(f'mongodb+srv://{username}:{password}@{cluster_name}/?retryWrites=true&w=majority')
  return client[database]

'''
  Deleta uma coleção
'''
def drop_collection(db: Database, collection_name: str) -> dict:
  return db.drop_collection(collection_name)

'''
  Insere um documento em uma coleção
'''
def insert_one(db: Database, collection_name: str, document: dict):
  return db[collection_name].insert_one(document).inserted_id

'''
  Insere vários documentos em uma coleção
'''
def insert_many(db: Database, collection_name: str, documents: list) -> list:
  return db[collection_name].insert_many(documents).inserted_ids

'''
  Busca por um documento
'''
def find_one(db: Database, collection_name: str, query: dict) -> dict:
  return db[collection_name].find_one(query)

'''
  Busca por mais de documento
'''
def find(db: Database, collection_name: str, query: dict) -> Cursor:
  return db[collection_name].find(query)

'''
  Deletar um documento
'''
def delete_one(db: Database, collection_name: str, query: dict) -> DeleteResult:
  return db[collection_name].delete_one(query)
