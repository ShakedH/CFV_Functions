import os
import sys
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'env/Lib/site-packages')))
import pyodbc
from azure.storage.table import TableService

_postreqdata = json.loads(open(os.environ['req']).read())

storage_acc_name = 'cfvtes9c07'
storage_acc_key = 'DSTJn6a1dS9aaoJuuw6ZOsnrsiW9V1jODJyHtekkYkc3BWofGVQjS6/ICWO7v51VUpTHSoiZXVvDI66uqTnOJQ=='


def get_videos_by_term(search_term):
    vid_ids = get_video_ids_by_term(search_term)
    if len(vid_ids) == 0:
        return {}
    video_info = get_video_info_by_vid_ids(vid_ids)
    return video_info


def get_video_ids_by_term(search_term):
    table_service = TableService(account_name=storage_acc_name, account_key=storage_acc_key)
    vid_ids = table_service.query_entities(table_name='CorpusInvertedIndex',
                                     filter='PartitionKey eq \'' + search_term + '\'',
                                     select='RowKey')
    if not vid_ids.items or len(vid_ids.items) == 0:
        return []
    video_ids = {record['RowKey'] for record in vid_ids.items}
    return video_ids


def get_video_info_by_vid_ids(vid_ids):
    cnxn = get_sql_cnxn()
    cursor = cnxn.cursor()
    list_vid_ids = list(vid_ids)
    ids_as_string = ','.join('\'{0}\''.format(id) for id in list_vid_ids)
    query = "SELECT * FROM {0} WHERE vid_id in ({1})"
    query = query.format('VideosMetaData', ids_as_string)
    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    data = cursor.fetchall()
    if not data or len(data) == 0:
        return {}
    results = [dict(zip(columns, row)) for row in data]
    return results


def get_sql_cnxn():
    server = 'cfvtest.database.windows.net'
    database = 'cfvtest'
    username = 'drasco'
    server_password = 'testTest1'
    driver = '{ODBC Driver 13 for SQL Server}'
    cnxn = pyodbc.connect(
        'DRIVER=' + driver + ';PORT=1433;SERVER=' + server + ';PORT=1443;DATABASE=' + database + ';UID=' + username + ';PWD=' + server_password)
    return cnxn

if __name__ == '__main__':
    print ("new request")
    search_term = _postreqdata['searchKey'].lower()
    dic = get_videos_by_term(search_term)
    response = open(os.environ['res'], 'w')
    response.write(json.dumps(dic))
    response.close()

