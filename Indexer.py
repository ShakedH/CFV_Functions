import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'env/Lib/site-packages')))
import json
import base64
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


def create_video_inverted_index(transcript_file_name):
    transcript_file = block_blob_service.get_blob_to_text(transcript_container_name, transcript_file_name)
    transcript_dict = json.loads(transcript_file.content)
    # transcript_dict = json.loads(open(transcript_file_name).read())  # For local testing
    video_inverted_index = {}
    for result_block in transcript_dict['response']['results']:
        alternative = result_block['alternatives'][0]
        paragraph = alternative['transcript']
        word_index = -1
        for word_info in alternative['words']:
            word_index += 1
            start_time = word_info['startTime'][:-1]
            word = word_info['word']
            parsed_word = parse_word(word)
            if not parsed_word:
                continue
            if parsed_word not in video_inverted_index:
                video_inverted_index[parsed_word] = {}
            video_inverted_index[parsed_word][start_time] = get_sentence(word_index, paragraph)
    return video_inverted_index


def update_inverted_indexes_azure_table(vid_id, video_inverted_index):
    for term in video_inverted_index:
        entity = Entity()
        entity.PartitionKey = vid_id
        entity.RowKey = urllib.quote_plus(term)
        entity.Status = 'Unscanned'
        appearances = {}
        for timestamp in video_inverted_index[term]:
            sentence = video_inverted_index[term][timestamp]
            appearances[timestamp] = sentence
        entity.Appearances = json.dumps(appearances)
        table_service.insert_entity('VideosInvertedIndexes', entity)


if __name__ == '__main__':
    queue_message = open(os.environ['inputMessage']).read()
    print(queue_message)
    # queue_message = base64.b64decode(queue_raw_message)
    # queue_message = queue_raw_message
    # queue_message = r'C:\Users\Ron Michaeli\Desktop\output.json.txt'  # For local testing
    print('Create video inverted index...')
    vii = create_video_inverted_index(queue_message)
    print('Updating Azure table...')
    update_inverted_indexes_azure_table(queue_message, vii)
    print('Done!')
