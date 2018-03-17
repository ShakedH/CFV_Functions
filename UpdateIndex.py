import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'env/Lib/site-packages')))
from azure.storage.table import TableService, Entity

account_name = 'cfvtes9c07'
account_key = 'DSTJn6a1dS9aaoJuuw6ZOsnrsiW9V1jODJyHtekkYkc3BWofGVQjS6/ICWO7v51VUpTHSoiZXVvDI66uqTnOJQ=='
table_service = TableService(account_name, account_key)
source_azure_table = 'VideosInvertedIndexes'
videos = set()

from smtplib import SMTP_SSL as SMTP
from email.mime.text import MIMEText
import pyodbc

def get_sql_cnxn():
    server = 'cfvtest.database.windows.net'
    database = 'cfvtest'
    username = 'drasco'
    server_password = 'testTest1'
    driver = '{ODBC Driver 13 for SQL Server}'
    cnxn = pyodbc.connect(
        'DRIVER=' + driver + ';PORT=1433;SERVER=' + server + ';PORT=1443;DATABASE=' + database + ';UID=' + username + ';PWD=' + server_password)
    return cnxn


def get_mails():
    cnxn = get_sql_cnxn()
    cursor = cnxn.cursor()
    query = 'SELECT email from Users'
    cursor.execute(query)
    mails = cursor.fetchall()
    return mails


def get_title(id):
    cnxn = get_sql_cnxn()
    cursor = cnxn.cursor()
    query = 'SELECT title from VideosMetaData where vid_id = \'{0}\''
    query = query.format(id)
    cursor.execute(query)
    title = cursor.fetchone()[0]
    return title


def send_email(message, recipient):
    HOST = 'smtp.gmail.com'
    PORT = 465
    USERNAME = 'cfvtest17@gmail.com'
    PASSWORD = 'cfvtest123'
    SENDER = 'Ctrl-FV'
    RECIPIENT = recipient
    text_subtype = 'plain'

    msg = MIMEText(message, text_subtype)
    msg['Subject'] = 'New video added to Ctrl-FV!'
    msg['From'] = SENDER
    msg['To'] = RECIPIENT
    try:
        connection = SMTP(HOST, PORT)
        connection.login(USERNAME, PASSWORD)
        connection.sendmail(SENDER, RECIPIENT, msg.as_string())
    except Exception as e:
        print(e)

def notify_new_vid(vid_id):
    try:
        title = get_title(vid_id)
        print(title)
        mails = get_mails()
        message = 'New video added to Ctrl-FV.\n Video Name: ' + title
        for mail in mails:
            mail = mail[0]
            print(mail)
            send_email(message, mail)
    except Exception as e:
        print(e)

def update_corpus_inverted_index():
    new_entites = table_service.query_entities(source_azure_table, filter="Status eq 'Unscanned'")
    for new_entity in new_entites:
        corpus_entity = Entity()
        corpus_entity.PartitionKey = new_entity.RowKey
        corpus_entity.RowKey = new_entity.PartitionKey
        table_service.insert_or_replace_entity('CorpusInvertedIndex', corpus_entity)
        new_entity.Status = 'Scanned'
        table_service.update_entity(source_azure_table, new_entity)
        videos.add(new_entity.PartitionKey)


if __name__ == '__main__':
    update_corpus_inverted_index()
    # for video in videos:
    #     print(video)
    #     notify_new_vid(video)
