import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'env/Lib/site-packages')))
import json
import numpy as np
import nltk
import urllib

nltk.download('stopwords')
from nltk.corpus import stopwords
from azure.storage.blob import BlockBlobService
from azure.storage.table import TableService, Entity

account_name = 'cfvtes9c07'
account_key = 'DSTJn6a1dS9aaoJuuw6ZOsnrsiW9V1jODJyHtekkYkc3BWofGVQjS6/ICWO7v51VUpTHSoiZXVvDI66uqTnOJQ=='
transcript_container_name = 'transcriptscontainer'
videos_container_name = 'videoscontainer'
block_blob_service = BlockBlobService(account_name, account_key)
table_service = TableService(account_name, account_key)
stop_words = set(stopwords.words('english'))
context_margin = 5


def parse_word(word_to_parse):
    return word_to_parse.lower() if word_to_parse not in stop_words and len(word_to_parse) > 1 else None


def get_sentence(word_index, paragraph):
    words = np.array(paragraph.split())
    min_index = max(0, word_index - context_margin)
    max_index = min(len(words), min_index + 2 * context_margin)
    indices = range(min_index, max_index)
    split_sentence = words[indices]
    return ' '.join(split_sentence)


def create_video_inverted_index(transcript, timestamps):
    video_inverted_index = {}
    word_index = -1
    for word_data in timestamps:
        word_index += 1
        start_time = word_data[1]
        word = word_data[0]
        parsed_word = parse_word(word)
        if not parsed_word:
            continue
        if parsed_word not in video_inverted_index:
            video_inverted_index[parsed_word] = {}
        video_inverted_index[parsed_word][start_time] = get_sentence(word_index, transcript)
    return video_inverted_index


def update_inverted_indexes_azure_table(vid_id, video_inverted_index):
    for term in video_inverted_index:
        try:
            entity = Entity()
            entity.PartitionKey = vid_id
            entity.RowKey = urllib.quote_plus(term)
            entity.Status = 'Unscanned'
            for timestamp in video_inverted_index[term]:
                sentence = video_inverted_index[term][timestamp]
                # property name for start time 21.19 will be t_21_19
                entity['t_' + str(timestamp).replace('.', '_')] = sentence
            table_service.insert_or_merge_entity('VideosInvertedIndexes', entity)
        except Exception as e:
            print ('Failed adding term ' + term)
            print(e)


if __name__ == '__main__':
    # inputMessage = open(os.environ['inputMessage']).read()
    # message_obj = json.loads(inputMessage)
    message_obj = {"file_name": "0003.wav", "transcript": "N. injuring a caddy", "ID": "test",
                   "timestamps": [["N.", 40.87, 21.11], ["injuring", 41.11, 21.77], ["a", 41.77, 21.91],
                                  ["caddy", 41.91, 22.57]]}
    ID = message_obj['ID']
    transcript = message_obj['transcript']
    timestamps = message_obj['timestamps']
    print('Create video inverted index...')
    vii = create_video_inverted_index(transcript=transcript, timestamps=timestamps)
    print('Updating Azure table...')
    update_inverted_indexes_azure_table(ID, vii)
    print('Done!')
