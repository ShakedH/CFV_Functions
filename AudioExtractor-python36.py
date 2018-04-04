import subprocess
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'myenv/Lib/site-packages')))
import numpy as np
from azure.storage.blob import BlockBlobService
from azure.storage.queue import QueueService
import json
import base64

account_name = 'cfvtes9c07'
account_key = 'DSTJn6a1dS9aaoJuuw6ZOsnrsiW9V1jODJyHtekkYkc3BWofGVQjS6/ICWO7v51VUpTHSoiZXVvDI66uqTnOJQ=='
audio_container_name = "audiocontainer"
block_blob_service = BlockBlobService(account_name, account_key)
queue_service = QueueService(account_name=account_name, account_key=account_key)
videos_container_URL = "https://cfvtes9c07.blob.core.windows.net/videoscontainer"
queue_name = "extractor-to-splitter-q"


def extract_audio_from_video(video_id):
    input_path = videos_container_URL + "/" + video_id
    print ('input_path: ' + input_path)
    command = ['ffmpeg',
               '-i', input_path,
               '-vn',
               '-f', 'mp3',
               'pipe:1'
               ]
    pipe = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdoutdata, stderr = pipe.communicate()
    audio_array = np.fromstring(stdoutdata, dtype="int16")
    audio_blob_name = video_id.replace('.mp4', '.mp3')
    print (' received ffmpeg output byte array')
    block_blob_service.create_blob_from_bytes(audio_container_name, audio_blob_name, audio_array.tobytes())
    print ('uploaded audio file to blob')
    return audio_blob_name


def put_message_in_queue(queue_name, video_id, audio_blob_name):
    print ('Creating message for queue:' + queue_name)
    message = {"video_id": video_id, "audio_blob_name": audio_blob_name}
    message = json.dumps(message)
    message = base64.b64encode(message.encode("ascii")).decode()
    queue_service.put_message(queue_name, message)
    print ("Sent message:" + message)


if __name__ == "__main__":
    video_id = open(os.environ["inputMessage"]).read()
    audio_blob_name = extract_audio_from_video(video_id)
    put_message_in_queue(queue_name, video_id, audio_blob_name)
