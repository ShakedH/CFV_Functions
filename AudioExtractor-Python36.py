import subprocess
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../myenv/Lib/site-packages')))
import numpy as np
from azure.storage.blob import BlockBlobService
from azure.storage.queue import QueueService
import json
import base64
import io
import scipy.io.wavfile as wavfile

account_name = 'ctrlfvfunctionaa670'
account_key = 'MoPjP9rLlfN8nK4+uejH6fSCwZHOqqvvfwVa6Ais3emwtGlly59oCS2Z8VQ+8OiKzzVwMghRImUPddVyMPAN9Q=='
audio_container_name = "audio-container"
block_blob_service = BlockBlobService(account_name, account_key)
queue_service = QueueService(account_name=account_name, account_key=account_key)
videos_container_URL = "https://ctrlfvfunctionaa670.blob.core.windows.net/video-container"
outgoing_msg_queue_name = "extractor-to-asr-q"
img_gen_queue_name = "extractor-to-img-gen-q"


def extract_audio_from_video(video_id):
    input_path = videos_container_URL + "/" + video_id
    print('input_path: ' + input_path)
    command = ['ffmpeg',
               '-i', input_path,
               '-vn',
               '-f', 'wav',
               'pipe:1'
               ]
    pipe = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdoutdata, stderr = pipe.communicate()
    stdoutdata = bytes(stdoutdata)
    riff_chunk_size = len(stdoutdata) - 8
    q = riff_chunk_size
    print('riff size' + str(q))
    b = []
    for i in range(4):
        q, r = divmod(q, 256)
        b.append(r)
    riff = stdoutdata[:4] + bytes(b) + stdoutdata[8:]
    rate, audio_array = wavfile.read(io.BytesIO(riff))
    duration_seconds = len(audio_array) / rate
    # audio_array = np.fromstring(stdoutdata, dtype="int16")
    audio_blob_name = os.path.splitext(video_id)[0] + '.wav'
    print('received ffmpeg output byte array')
    block_blob_service.create_blob_from_bytes(audio_container_name, audio_blob_name, riff)
    print('uploaded audio file to blob')
    return audio_blob_name, duration_seconds


def put_message_in_outgoing_queue(queue_name, video_id, audio_blob_name, duration_seconds):
    print('Creating message for outgoing queue')
    message = {"ID": video_id, "file_name": audio_blob_name, "duration": duration_seconds}
    message = json.dumps(message)
    put_message_in_queue(message, queue_name)


def put_message_in_queue(message, queue_name):
    print('creating msg for queue' + queue_name)
    message = base64.b64encode(message.encode("ascii")).decode()
    queue_service.put_message(queue_name, message)
    print("Sent message:" + message)


if __name__ == "__main__":
    video_id = open(os.environ["inputMessage"]).read()
    put_message_in_queue(video_id, img_gen_queue_name)
    audio_blob_name, duration_seconds = extract_audio_from_video(video_id)
    put_message_in_outgoing_queue(outgoing_msg_queue_name, video_id, audio_blob_name, duration_seconds)
