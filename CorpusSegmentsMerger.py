import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'env/Lib/site-packages')))
from urllib2 import urlopen
import json
from azure.storage.blob import BlockBlobService


def get_segments_dic_from_container(id):
    account_name = 'cfvtes9c07'
    corpus_seg_container_name = "corpus-segments-container"
    dic_file_url = r"https://{0}.blob.core.windows.net/{1}/{2}".format(account_name, corpus_seg_container_name,id)
    dic_file_object = urlopen(dic_file_url)
    dic_json = json.loads(dic_file_object.read())
    return dic_json

def write_full_transcript_to_blob(id,transcript):
    print ("saving transcript as blob...")
    account_name = 'cfvtes9c07'
    account_key = 'DSTJn6a1dS9aaoJuuw6ZOsnrsiW9V1jODJyHtekkYkc3BWofGVQjS6/ICWO7v51VUpTHSoiZXVvDI66uqTnOJQ=='
    corpus_seg_container_name = "corpus-container"
    blob_name = id
    block_blob_service = BlockBlobService(account_name, account_key)
    block_blob_service.create_blob_from_text(corpus_seg_container_name, blob_name, json.dumps(transcript))
    print ("blob:  %s saved." % blob_name)


if __name__ == '__main__':
    # test
    #message_obj = {"ID": "Test1"}
    inputMessage = open(os.environ['inputMessage']).read()
    message_obj = json.loads(inputMessage)
    ID = message_obj['ID']
    segments_dic = get_segments_dic_from_container(ID)
    sorted_segments_dic = {k: segments_dic[k] for k in sorted(segments_dic)}
    transcript = " ".join(sorted_segments_dic.values())
    write_full_transcript_to_blob(ID, transcript)












# vid_id = "test"
#
# service = TableService(account_name=storage_acc_name, account_key=storage_acc_key)
# service = TableService(account_name=storage_acc_name, account_key=storage_acc_key)
# terms = service.query_entities(table_name='VideosInvertedIndexes',
#                                    filter='PartitionKey eq \'' + vid_id + '\'',
#                                    select='*')
#
# time_term_dic = {}
# if not terms.items:
#     raise Exception('No terms for Video ID {} '.format(vid_id))
#
#
# for record in terms.items:
#     current_term = str(record['RowKey'])
#     for column in record:
#         if column.startswith("t_"):
#             for char in ["t",'_'] : column = column.replace(char, "")
#             time_term_dic[int(column)] = current_term
#
# sorted_time_term_dic = {k: time_term_dic[k] for k in sorted(time_term_dic)}
#
# transcript = " ".join(sorted_time_term_dic.values())
# print (transcript)