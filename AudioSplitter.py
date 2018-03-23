import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'env/Lib/site-packages')))
from azure.storage.blob import BlockBlobService
from pydub import AudioSegment
from pydub import silence
from urllib2 import urlopen

account_name = 'cfvtes9c07'
account_key = 'DSTJn6a1dS9aaoJuuw6ZOsnrsiW9V1jODJyHtekkYkc3BWofGVQjS6/ICWO7v51VUpTHSoiZXVvDI66uqTnOJQ=='
audio_container_name = "audiocontainer"
block_blob_service = BlockBlobService(account_name, account_key)


def split_audio_file(audio_file_name):
    audio_file_url = r"https://{0}.blob.core.windows.net/{1}/{2}".format(account_name, audio_container_name,
                                                                         audio_file_name)
    print audio_file_url
    audio_file_object = urlopen(audio_file_url)
    audio_segment = AudioSegment(audio_file_object)
    print 'Im all good'


if __name__ == "__main__":
    queue_message = open(os.environ["inputMessage"]).read()
    queue_message = 'english-2Minutes.wav'  # for local testing
    split_audio_file(queue_message)
