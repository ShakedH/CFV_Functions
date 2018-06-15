import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../myenv/Lib/site-packages')))
import json
import numpy as np
import urllib
from our_stopwords import stop_words
from azure.storage.blob import BlockBlobService
from azure.cosmosdb.table import TableService, Entity

account_name = 'ctrlfvfunctionaa670'
account_key = 'MoPjP9rLlfN8nK4+uejH6fSCwZHOqqvvfwVa6Ais3emwtGlly59oCS2Z8VQ+8OiKzzVwMghRImUPddVyMPAN9Q=='
transcript_container_name = 'transcript-container'
videos_container_name = 'video-container'
block_blob_service = BlockBlobService(account_name, account_key)
table_service = TableService(account_name, account_key)
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
        if not parsed_word or parsed_word == "%hesitation":
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
            entity.RowKey = urllib.parse.quote_plus(term)
            for timestamp in video_inverted_index[term]:
                sentence = video_inverted_index[term][timestamp]
                # property name for start time 21.19 will be t_21_19
                entity['t_' + str(timestamp).replace('.', '_')] = sentence
            table_service.insert_or_merge_entity('VideosInvertedIndexes', entity)
        except Exception as e:
            print('Failed adding term', term)
            print(e)


def update_video_index_progress_table(ID, total_segments, index):
    try:
        entity = Entity()
        entity.PartitionKey = ID + '_' + str(int(index) // 200)
        entity.RowKey = total_segments
        entity['t_' + str(index)] = index
        print('entity #' + str(index))
        table_service.merge_entity('VideosIndexProgress', entity)
    except Exception as e:
        print(e)


if __name__ == '__main__':
    inputMessage = open(os.environ['inputMessage']).read()
    message_obj = json.loads(inputMessage)
    ID = message_obj['ID']
    transcript = message_obj['transcript']
    timestamps = message_obj['timestamps']
    total_segments = message_obj['total_segments']
    index = message_obj['index']
    print('Create video inverted index...')
    vii = create_video_inverted_index(transcript=transcript, timestamps=timestamps)
    print('Updating Azure table...')
    update_inverted_indexes_azure_table(ID, vii)
    print('Updating Progress table...')
    update_video_index_progress_table(ID, total_segments, index)
    print('Done!')
