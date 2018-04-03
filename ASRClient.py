import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'env/Lib/site-packages')))

import base64
import itertools
import json
from urllib import urlencode
import speech_recognition as sr
from urllib2 import urlopen, Request, HTTPError, URLError
from azure.storage.queue import QueueService
import threading


def recognize_ibm(audio_data, username, password, language="en-US", show_all=False):
    """
    Performs speech recognition on ``audio_data`` (an ``AudioData`` instance), using the IBM Speech to Text API.

    The IBM Speech to Text username and password are specified by ``username`` and ``password``, respectively. Unfortunately, these are not available without `signing up for an account <https://console.ng.bluemix.net/registration/>`__. Once logged into the Bluemix console, follow the instructions for `creating an IBM Watson service instance <https://www.ibm.com/watson/developercloud/doc/getting_started/gs-credentials.shtml>`__, where the Watson service is "Speech To Text". IBM Speech to Text usernames are strings of the form XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX, while passwords are mixed-case alphanumeric strings.

    The recognition language is determined by ``language``, an RFC5646 language tag with a dialect like ``"en-US"`` (US English) or ``"zh-CN"`` (Mandarin Chinese), defaulting to US English. The supported language values are listed under the ``model`` parameter of the `audio recognition API documentation <https://www.ibm.com/watson/developercloud/speech-to-text/api/v1/#sessionless_methods>`__, in the form ``LANGUAGE_BroadbandModel``, where ``LANGUAGE`` is the language value.

    Returns the most likely transcription if ``show_all`` is false (the default). Otherwise, returns the `raw API response <https://www.ibm.com/watson/developercloud/speech-to-text/api/v1/#sessionless_methods>`__ as a JSON dictionary.

    Raises a ``speech_recognition.UnknownValueError`` exception if the speech is unintelligible. Raises a ``speech_recognition.RequestError`` exception if the speech recognition operation failed, if the key isn't valid, or if there is no internet connection.
    """
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
    if show_all: return result
    if "results" not in result or len(result["results"]) < 1 or "alternatives" not in result["results"][0]:
        raise sr.UnknownValueError()

    transcription = []
    for utterance in result["results"]:
        if "alternatives" not in utterance: raise sr.UnknownValueError()
        for hypothesis in utterance["alternatives"]:
            if "transcript" in hypothesis:
                transcription.append(hypothesis["transcript"])
    return "\n".join(transcription)


def get_transcript(file_name):
    IBM_USERNAME = "853a3e00-bd09-4d31-8b78-312058948303"  # IBM Speech to Text usernames are strings of the form XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX
    IBM_PASSWORD = "YOBwYe01gUeG"  # IBM Speech to Text passwords are mixed-case alphanumeric strings
    storage_account_name = 'cfvtes9c07'
    audio_container_name = "audio-segments-container"
    audio_file_url = r"https://{0}.blob.core.windows.net/{1}/{2}".format(storage_account_name, audio_container_name,
                                                                         file_name)
    audio_obj = urlopen(audio_file_url)

    r = sr.Recognizer()
    with sr.AudioFile(audio_obj) as source:
        audio = r.record(source)  # read the entire audio file
    results = recognize_ibm(audio_data=audio, username=IBM_USERNAME, password=IBM_PASSWORD,
                            show_all=True)
    data = {"file_name": file_name}
    data['transcript'] = '. '.join(
        [result['alternatives'][0]['transcript'].strip() for result in results['results']])
    data['timestamps'] = list(itertools.chain.from_iterable(
        [results['alternatives'][0]['timestamps'] for results in results['results']]))
    return data


def update_start_time(data, start_time):
    new_data = data.copy()
    new_timestamps = [[record[0], record[1] + start_time, record[2] + start_time] for record in data['timestamps']]
    new_data['timestamps'] = new_timestamps
    return new_data


def enqueue_message(qname, message):
    storage_acc_name = 'cfvtes9c07'
    storage_acc_key = 'DSTJn6a1dS9aaoJuuw6ZOsnrsiW9V1jODJyHtekkYkc3BWofGVQjS6/ICWO7v51VUpTHSoiZXVvDI66uqTnOJQ=='
    message = base64.b64encode(message.encode('ascii')).decode()
    queue_service = QueueService(account_name=storage_acc_name, account_key=storage_acc_key)
    queue_service.put_message(qname, message)


def process_segment(file, start, q_name):
    print('started function. File Name: ' + file_name + '. Start_time: ' + str(start_time))
    data = get_transcript(file_name=file)
    data = update_start_time(data, start)
    print('Finished segment starting in ' + start)
    enqueue_message(q_name, json.dumps(data))


if __name__ == '__main__':
    inputMessage = open(os.environ['inputMessage']).read()
    message_obj = json.loads(inputMessage)
    files = message_obj['files']
    print('Started processing files')
    for file in files:
        try:
            file_name = file['file_name']
            start_time = file['start_time']
            threading.Thread(target=process_segment, args=(file_name, start_time, 'indexq'))
        except Exception as e:
            print e
