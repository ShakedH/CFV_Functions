import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'env/Lib/site-packages')))
from azure.storage.blob import BlockBlobService
from azure.storage.queue import QueueService
from azure.storage.blob.models import ContentSettings
from urllib2 import urlopen
from pydub import AudioSegment
import json
import base64
import numpy as np

account_name = "cfvtes9c07"
account_key = "DSTJn6a1dS9aaoJuuw6ZOsnrsiW9V1jODJyHtekkYkc3BWofGVQjS6/ICWO7v51VUpTHSoiZXVvDI66uqTnOJQ=="
audio_container_name = "audiocontainer"
audio_segments_container_name = "audio-segments-container"
queue_name = "splitter-to-asr-q"
block_blob_service = BlockBlobService(account_name, account_key)
queue_service = QueueService(account_name=account_name, account_key=account_key)


def get_audio_file_from_container(audio_file_name):
    audio_file_url = r"https://{0}.blob.core.windows.net/{1}/{2}".format(account_name, audio_container_name, audio_file_name)
    audio_file_object = urlopen(audio_file_url)
    # return AudioSegment(audio_file_object)

    # region Testing
    # a = audio_file_object.read()
    # len(a) = 44 + (4 * frame_rate * duration) not sure if always 4 or (sample_width * channels)
    # duration = (len(a) - 44) / (4 * frame_rate)
    # import wave
    # w_file = wave.open(audio_file_object)
    block_blob_service.create_blob_from_stream(container_name=audio_segments_container_name,
                                               blob_name="RonTest_c.wav",
                                               stream=audio_file_object)
    # w_file.close()
    print ""
    # endregion


def split_audio_file_by_silence(audio_segment):
    from pydub import silence
    # pydub.silence API: https://github.com/jiaaro/pydub/blob/master/pydub/silence.py

    min_silence_len_param = 1000  # window size
    silence_thresh_param = -36
    seek_step_param = 1000  # window offset step
    keep_silence_param = 0
    # print silence.detect_nonsilent(audio_segment,
    #                                min_silence_len=min_silence_len_param,
    #                                silence_thresh=silence_thresh_param,
    #                                seek_step=seek_step_param)
    return silence.split_on_silence(audio_segment,
                                    min_silence_len=min_silence_len_param,
                                    silence_thresh=silence_thresh_param,
                                    seek_step=seek_step_param,
                                    keep_silence=keep_silence_param)


def split_audio_file_by_time(audio_segment, time_interval_seconds):
    segment_start = 0
    audio_segments = []
    audio_starts = []
    while segment_start < audio_segment.duration_seconds:
        segment_end = min(segment_start + time_interval_seconds, audio_segment.duration_seconds)
        segment = audio_segment[segment_start * 1000:segment_end * 1000]
        audio_segments.append(segment)
        audio_starts.append(segment_start)
        segment_start = segment_end
    return audio_segments, audio_starts


def upload_segments_to_container(full_audio_file_name, audio_segments, dest_container_name):
    audio_file_name = os.path.splitext(full_audio_file_name)[0]  # file name without extension
    audio_file_extension = os.path.splitext(full_audio_file_name)[1]  # file extension
    # content_settings = ContentSettings(content_type="audio/x-wav")
    blob_names = []
    i = 1
    for audio_segment in audio_segments:
        blob_name = "{0}_{1}{2}".format(audio_file_name, i, audio_file_extension)
        audio_data = np.fromstring(audio_segment.raw_data, dtype="int16")
        block_blob_service.create_blob_from_bytes(container_name=dest_container_name,
                                                  blob_name=blob_name,
                                                  blob=audio_data.tobytes())
        blob_names.append(blob_name)
        i += 1
    return blob_names


def put_message_in_queue(queue_name, video_id, audio_starts, blob_names):
    if len(audio_starts) != len(blob_names):
        raise Exception("audio_starts list and blob_names list are not the same size! CHECK IT")

    message = {"ID": video_id, "files": []}
    for i in range(len(audio_starts)):
        blob_name_and_start_tuple = {"file_name": blob_names[i], "start_time": audio_starts[i]}
        message["files"].append(blob_name_and_start_tuple)
    message = json.dumps(message)
    message = base64.b64encode(message.encode("ascii")).decode()
    queue_service.put_message(queue_name, message)


def delete_extracted_audio_file(audio_file_name):
    block_blob_service.delete_blob(container_name=audio_container_name, blob_name=audio_file_name)


if __name__ == "__main__":
    # audio_file_name = open(os.environ["inputMessage"]).read()
    audio_file_name = "english-2Minutes.wav"  # for local testing
    video_id = "english-2Minutes.mp4"  # for local testing
    # region Prints
    print "File name:", audio_file_name
    print "Downloading audio file from {}...".format(audio_container_name)
    # endregion
    audio_file = get_audio_file_from_container(audio_file_name)
    # region Prints
    print "Splitting file to segments..."
    # endregion
    # audio_segments = split_audio_file_by_silence(audio_segment)
    time_interval_seconds = 10
    audio_segments, audio_starts = split_audio_file_by_time(audio_file, time_interval_seconds)
    # region Prints
    print "Split {0} seconds to {1} segments of {2} seconds".format(int(audio_file.duration_seconds), len(audio_segments), time_interval_seconds)
    print "Uploading segments to", audio_segments_container_name, "..."
    # endregion
    blob_names = upload_segments_to_container(audio_file_name, audio_segments, audio_segments_container_name)
    # region Prints
    print "Putting message in {0}...".format(queue_name)
    # endregion
    put_message_in_queue(queue_name, video_id, audio_starts, blob_names)
    # region Prints
    print "Deleting extracted audio file..."
    # endregion
    # delete_extracted_audio_file(audio_file_name)
    # region Prints
    print "Done!"
    # endregion
