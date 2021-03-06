import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../myenv/Lib/site-packages')))

import math
import json
import base64
import requests
import itertools
import speech_recognition as sr
from threading import Thread
from urllib.parse import urlencode
from urllib.request import urlopen, Request, HTTPError, URLError
from azure.storage.queue import QueueService
from azure.cosmosdb.table import TableService, Entity
from azure.storage.blob import BlockBlobService, PublicAccess

storage_acc_name = 'ctrlfvfunctionaa670'
storage_acc_key = 'MoPjP9rLlfN8nK4+uejH6fSCwZHOqqvvfwVa6Ais3emwtGlly59oCS2Z8VQ+8OiKzzVwMghRImUPddVyMPAN9Q=='
table_service = TableService(storage_acc_name, storage_acc_key)


# region STT
def recognize_ibm(audio_data, username, password, language="en-US", show_all=False):
    assert isinstance(audio_data, sr.AudioData), "Data must be audio data"
    assert isinstance(username, str), "``username`` must be a string"
    assert isinstance(password, str), "``password`` must be a string"

    flac_data = audio_data.get_flac_data(
        convert_rate=None if audio_data.sample_rate >= 16000 else 16000,  # audio samples should be at least 16 kHz
        convert_width=None if audio_data.sample_width >= 2 else 2  # audio samples should be at least 16-bit
    )
    url = "https://stream.watsonplatform.net/speech-to-text/api/v1/recognize?{}".format(urlencode({
        "profanity_filter": "false",
        "model": "{}_BroadbandModel".format(language),
        "inactivity_timeout": -1,  # don't stop recognizing when the audio stream activity stops
        "timestamps": "true"
    }))
    request = Request(url, data=flac_data, headers={
        "Content-Type": "audio/x-flac",
        "X-Watson-Learning-Opt-Out": "true",  # prevent requests from being logged, for improved privacy
    })
    authorization_value = base64.standard_b64encode("{}:{}".format(username, password).encode("utf-8")).decode("utf-8")
    request.add_header("Authorization", "Basic {}".format(authorization_value))
    try:
        response = urlopen(request, timeout=None)
    except HTTPError as e:
        raise sr.RequestError("recognition request failed: {}".format(e.reason))
    except URLError as e:
        raise sr.RequestError("recognition connection failed: {}".format(e.reason))
    response_text = response.read().decode("utf-8")
    result = json.loads(response_text)

    # return results
    if show_all:
        return result
    if "results" not in result or len(result["results"]) < 1 or "alternatives" not in result["results"][0]:
        raise sr.UnknownValueError()

    transcription = []
    for utterance in result["results"]:
        if "alternatives" not in utterance:
            raise sr.UnknownValueError()
        for hypothesis in utterance["alternatives"]:
            if "transcript" in hypothesis:
                transcription.append(hypothesis["transcript"])
    return "\n".join(transcription)


def get_transcript(audio):
    IBM_USERNAME = "b2953aea-1687-4545-ad0d-241dfe0de6c8"  # IBM Speech to Text usernames are strings of the form XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX
    IBM_PASSWORD = "Q7X0JQ2zyg5k"  # IBM Speech to Text passwords are mixed-case alphanumeric strings
    ibm_results = recognize_ibm(audio_data=audio, username=IBM_USERNAME, password=IBM_PASSWORD,
                                show_all=True)
    transcript_segments = []
    alternatives = []
    for result in ibm_results['results']:
        transcript_segments.append(result['alternatives'][0]['transcript'].strip())
        alternatives.append(result['alternatives'][0]['timestamps'])
        SEGMENTS_CONFIDENCE.append(result['alternatives'][0]['confidence'])

    data = {
        'transcript': '. '.join(transcript_segments),
        'timestamps': list(itertools.chain.from_iterable(alternatives))
    }

    return data


def update_start_time(data, start_time):
    new_data = data.copy()
    new_timestamps = [[record[0], record[1] + start_time, record[2] + start_time] for record in data['timestamps']]
    new_data['timestamps'] = new_timestamps
    return new_data


def enqueue_message(qname, message):
    message = base64.b64encode(message.encode('ascii')).decode()
    queue_service = QueueService(account_name=storage_acc_name, account_key=storage_acc_key)
    queue_service.put_message(qname, message)


def delete_blob(blob_name, container_name):
    block_blob_service = BlockBlobService(account_name=storage_acc_name, account_key=storage_acc_key)
    # Set the permission so the blobs are public.
    block_blob_service.set_container_acl(container_name, public_access=PublicAccess.Container)
    block_blob_service.delete_blob(container_name=container_name, blob_name=blob_name)


def process_segment(audio, ID, start_time, index, q_name):
    try:
        try:
            data = get_transcript(audio)
            data = update_start_time(data, start_time)
        except Exception:
            data = {'timestamps': [], 'transcript': ''}
        data['ID'] = ID
        data['total_segments'] = TOTAL_SEGMENTS
        data['index'] = index
        print('Ended processing segment starting in ' + str(start_time))
        enqueue_message(q_name, json.dumps(data))
        # add start time and transcript to dic
        _time_transcript_dic[int(start_time)] = data['transcript']
    except Exception as e:
        print(e)


# endregion

# region Confidence
def update_confidence_in_metadata(vid_id, confidence):
    print("Updating VMD with confidence...")
    server_url = 'https://ctrl-fvideos.azurewebsites.net'
    data = {'videoID': vid_id, 'columnName': 'confidence', 'columnValue': confidence}
    r = requests.post(server_url + "/updateVMD", data=json.dumps(data))
    print(r.status_code, r.reason)


# endregion

# region Transcript dictionary
_time_transcript_dic = {}


def save_dic_to_blob(vid_id):
    # save dic as blob
    account_name = 'ctrlfvfunctionaa670'
    account_key = 'MoPjP9rLlfN8nK4+uejH6fSCwZHOqqvvfwVa6Ais3emwtGlly59oCS2Z8VQ+8OiKzzVwMghRImUPddVyMPAN9Q=='
    corpus_seg_container_name = "corpus-segments-container"
    blob_name = vid_id + ".txt"
    print("saving dic as blob...")
    block_blob_service = BlockBlobService(account_name, account_key)
    block_blob_service.create_blob_from_text(corpus_seg_container_name, blob_name,
                                             json.dumps(list(_time_transcript_dic.items())))
    # add message to asr-to-CorpusSegMerger queue
    queue_service = QueueService(account_name=account_name, account_key=account_key)
    queue_name = "asr-to-corpus-seg-merger-q"
    print('Creating message for queue:' + queue_name)
    message = {"ID": blob_name}
    message = json.dumps(message)
    message = base64.b64encode(message.encode("ascii")).decode()
    queue_service.put_message(queue_name, message)
    print("Sent message:" + message)


# endregion

def main():
    print('Started function app')

    inputMessage = open(os.environ['inputMessage']).read()
    message_obj = json.loads(inputMessage)
    file_name = message_obj['file_name']
    vid_id = message_obj['ID']
    max_duration = float(message_obj['duration'])

    # region Debug
    # file_name = "Data_structures_Binary_Tree.wav"
    # vid_id = "Data_structures_Binary_Tree.mp4"
    # max_duration = 976.5
    # endregion

    print('Started processing file')

    audio_container_name = "audio-container"
    audio_file_url = r"https://{0}.blob.core.windows.net/{1}/{2}".format(storage_acc_name, audio_container_name,
                                                                         file_name)
    audio_obj = urlopen(audio_file_url)

    print('Finished reading file named:', file_name)

    r = sr.Recognizer()
    start = 0
    duration = 10.0
    segment_counter = 0

    global SEGMENTS_CONFIDENCE
    SEGMENTS_CONFIDENCE = []
    threads = []
    with sr.AudioFile(audio_obj) as source:
        # r.record doesn't read exactly 'duration' seconds of the audio source, but a bit more = actual_duration
        seconds_per_buffer = (source.CHUNK + 0.0) / source.SAMPLE_RATE
        buffers_per_duration = math.ceil(duration / seconds_per_buffer)
        actual_duration = round(seconds_per_buffer * buffers_per_duration, 2)

        global TOTAL_SEGMENTS
        TOTAL_SEGMENTS = math.ceil(max_duration / actual_duration)
        # // is div
        for i in range(int(TOTAL_SEGMENTS) // 200 + 1):
            entity = Entity()
            entity.PartitionKey = str(vid_id) + '_' + str(i)
            entity.RowKey = str(TOTAL_SEGMENTS)
            table_service.insert_entity('VideosIndexProgress', entity)

        print('Created records in VideosIndexProgress Table')

        while start < max_duration:
            audio = r.record(source, duration=min(max_duration - start,
                                                  duration))  # although 'duration' is passed, 'actual_duration' will be read
            t = Thread(target=process_segment, args=(audio, vid_id, start, segment_counter, 'asr-to-parser-q'))
            threads.append(t)
            t.start()
            start += actual_duration
            print("start time of segment:", str(start))
            segment_counter += 1
    for t in threads:
        t.join()
    save_dic_to_blob(vid_id)
    delete_blob(file_name, 'audio-container')

    print('Adding confidence to VideosMetaData table')

    confidence = sum(SEGMENTS_CONFIDENCE) / len(SEGMENTS_CONFIDENCE) if len(SEGMENTS_CONFIDENCE) != 0 else 0
    update_confidence_in_metadata(vid_id=vid_id, confidence=confidence)

    print('finished processing ' + str(len(threads)) + ' segments')


if __name__ == '__main__':
    main()
