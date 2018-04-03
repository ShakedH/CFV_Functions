import subprocess
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'myenv/Lib/site-packages')))
import numpy as np
from azure.storage.blob import BlockBlobService

account_name = 'cfvtes9c07'
account_key = 'DSTJn6a1dS9aaoJuuw6ZOsnrsiW9V1jODJyHtekkYkc3BWofGVQjS6/ICWO7v51VUpTHSoiZXVvDI66uqTnOJQ=='
audio_container_name = "audiocontainer"
block_blob_service = BlockBlobService(account_name, account_key)
containerUrl = "https://cfvtes9c07.blob.core.windows.net/videoscontainer"


def extract_audio_from_video(video_file_name):
    input_path = containerUrl + "/" + video_file_name
    print ('inputpath' + input_path)
    command = ['ffmpeg',
               '-i', input_path,
               '-vn',
               '-f', 'mp3',
               'pipe:1']
    pipe = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdoutdata, stderr = pipe.communicate()
    print (stderr)

    # pipe = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
    # stdoutdata = pipe.communicate()[0]
    audio_array = np.fromstring(stdoutdata, dtype="int16")
    audio_blob_name = video_file_name.replace('.mp4', '.mp3')
    print (' finished ffmpeg, before cloud storage')
    block_blob_service.create_blob_from_bytes(audio_container_name, audio_blob_name, audio_array.tobytes())
    print ('finished cloud')


if __name__ == "__main__":
    # queue_message = open(os.environ["inputMessage"]).read()
    extract_audio_from_video('ErezTest_25022018_2010.mp4')