import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'env/Lib/site-packages')))
from azure.cosmosdb.table import TableService, Entity

account_name = 'cfvtes9c07'
account_key = 'DSTJn6a1dS9aaoJuuw6ZOsnrsiW9V1jODJyHtekkYkc3BWofGVQjS6/ICWO7v51VUpTHSoiZXVvDI66uqTnOJQ=='
table_service = TableService(account_name, account_key)
source_azure_table = 'VideosInvertedIndexes'


def update_corpus_inverted_index():
    new_entites = table_service.query_entities(source_azure_table, filter="Status eq 'Unscanned'")
    for new_entity in new_entites:
        corpus_entity = Entity()
        corpus_entity.PartitionKey = new_entity.RowKey
        corpus_entity.RowKey = new_entity.PartitionKey
        table_service.insert_or_replace_entity('CorpusInvertedIndex', corpus_entity)
        new_entity.Status = 'Scanned'
        table_service.update_entity(source_azure_table, new_entity)


if __name__ == '__main__':
    update_corpus_inverted_index()
