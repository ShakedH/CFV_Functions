import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../myenv/Lib/site-packages')))
from azure.storage.blob import BlockBlobService
import numpy as np
import subprocess
import requests
import json

account_name = 'ctrlfvfunctionaa670'
account_key = 'MoPjP9rLlfN8nK4+uejH6fSCwZHOqqvvfwVa6Ais3emwtGlly59oCS2Z8VQ+8OiKzzVwMghRImUPddVyMPAN9Q=='
images_container_name = "image-container"
block_blob_service = BlockBlobService(account_name, account_key)
videos_container_URL = "https://ctrlfvfunctionaa670.blob.core.windows.net/video-container"


def extract_tn_from_video(vid_id):
    input_path = videos_container_URL + "/" + vid_id
    print('input_path: ' + input_path)
    command = ['ffmpeg',
               '-ss', '00:00:05',
               '-i', input_path,
               '-t', '1',
               '-s', '480x300',
               '-f', 'image2pipe',
               '-vcodec', 'png', '-']
    pipe = subprocess.Popen(command, stdout=subprocess.PIPE, bufsize=10 ** 8)
    raw_image = pipe.stdout.read(420 * 360 * 3)
    tn = np.fromstring(raw_image, dtype='uint8')
    tn_blob_name = os.path.splitext(vid_id)[0] + '.png'
    print('received ffmpeg output byte array')
    block_blob_service.create_blob_from_bytes(images_container_name, tn_blob_name, tn.tobytes())
    print('uploaded thumbnail to blob')
    return tn_blob_name


def update_tn_url_in_metadata(vid_id, tn_url):
    print("Updating VMD with thumbnail URL...")
    server_url = 'https://ctrl-fvideos.azurewebsites.net'
    data = {'videoID': vid_id, 'columnName': 'tn_url', 'columnValue': tn_url}
    r = requests.post(server_url + "/updateVMD", data=json.dumps(data))
    print(r.status_code, r.reason)


if __name__ == "__main__":
    video_id = open(os.environ["inputMessage"]).read()
    tn_name = extract_tn_from_video(video_id)
    tn_url = "https://ctrlfvfunctionaa670.blob.core.windows.net/{0}/{1}".format(images_container_name, tn_name)
    update_tn_url_in_metadata(video_id, tn_url)
