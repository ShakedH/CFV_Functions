import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'env/Lib/site-packages')))
import requests
from urllib2 import urlopen
import json
from azure.storage.blob import BlockBlobService, PublicAccess

server_url = 'https://cfvtest.azurewebsites.net'
account_name = 'cfvtes9c07'
account_key = 'DSTJn6a1dS9aaoJuuw6ZOsnrsiW9V1jODJyHtekkYkc3BWofGVQjS6/ICWO7v51VUpTHSoiZXVvDI66uqTnOJQ=='


def get_segments_dic_from_container(id):
    corpus_seg_container_name = "corpus-segments-container"
    dic_file_url = r"https://{0}.blob.core.windows.net/{1}/{2}".format(account_name, corpus_seg_container_name, id)
    dic_file_object = urlopen(dic_file_url)
    dic_json = dict(json.loads(dic_file_object.read()))
    return dic_json


def delete_blob(blob_name):
    container_name = "corpus-segments-container"
    block_blob_service = BlockBlobService(account_name, account_key)
    # Set the permission so the blobs are public.
    block_blob_service.set_container_acl(container_name, public_access=PublicAccess.Container)
    block_blob_service.delete_blob(container_name, blob_name)
    print (blob_name + " was deleted from corpus-segments-container")


def write_full_transcript_to_blob(id, transcript):
    print ("saving transcript as blob...")
    corpus_seg_container_name = "corpus-container"
    blob_name = id
    block_blob_service = BlockBlobService(account_name, account_key)
    block_blob_service.create_blob_from_text(corpus_seg_container_name, blob_name, json.dumps(transcript))
    print ("blob:  %s saved." % blob_name)


def call_server_index_update(blob_name):
    data = {'videoID': blob_name}
    # update corpus index
    r = requests.post(server_url + "/createUpdateWhooshIndex", data=json.dumps(data))
    print(r.status_code, r.reason)


if __name__ == '__main__':
    # for filename in ['agriculture.txt',"computer.txt","desert.txt","farm.txt","pc.txt","politics.txt" ]:
    #     call_server_inedex_update(filename)
    # test
    # message_obj = {"ID": "dan_test_delete.mp4.txt"}

    inputMessage = open(os.environ['inputMessage']).read()
    message_obj = json.loads(inputMessage)
    ID = message_obj['ID']
    segments_dic = get_segments_dic_from_container(ID)
    sorted_transcript_list = [segments_dic[k] for k in sorted(segments_dic.keys())]
    transcript = " ".join(sorted_transcript_list)
    write_full_transcript_to_blob(ID, transcript)
    call_server_index_update(ID)
    delete_blob(ID)
    print(transcript)
