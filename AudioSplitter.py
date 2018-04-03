import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'env/Lib/site-packages')))
from azure.storage.blob import BlockBlobService
from azure.storage.blob.models import ContentSettings
from urllib2 import urlopen
from pydub import AudioSegment

account_name = "cfvtes9c07"
account_key = "DSTJn6a1dS9aaoJuuw6ZOsnrsiW9V1jODJyHtekkYkc3BWofGVQjS6/ICWO7v51VUpTHSoiZXVvDI66uqTnOJQ=="
audio_container_name = "audiocontainer"
audio_segments_container_name = "audio-segments-container"
block_blob_service = BlockBlobService(account_name, account_key)


def get_audio_file_from_container(audio_file_name):
    audio_file_url = r"https://{0}.blob.core.windows.net/{1}/{2}".format(account_name, audio_container_name, audio_file_name)
    audio_file_object = urlopen(audio_file_url)
    return AudioSegment(audio_file_object)


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
    while segment_start < audio_segment.duration_seconds:
        segment_end = min(segment_start + time_interval_seconds, audio_segment.duration_seconds)
        segment = audio_segment[segment_start * 1000:segment_end * 1000]
        audio_segments.append(segment)
        segment_start = segment_end
    return audio_segments


def upload_segments_to_container(full_audio_file_name, audio_segments, dest_container_name):
    audio_file_name = os.path.splitext(full_audio_file_name)[0]  # file name without extension
    audio_file_extension = os.path.splitext(full_audio_file_name)[1]  # file extension
    content_settings = ContentSettings(content_type="audio/x-wav")
    i = 1
    for audio_segment in audio_segments:
        block_blob_service.create_blob_from_bytes(container_name=dest_container_name,
                                                  blob_name="{0}_{1}{2}".format(audio_file_name, i, audio_file_extension),
                                                  blob=bytes(audio_segment.raw_data),
                                                  content_settings=content_settings)
        i += 1


if __name__ == "__main__":
    # audio_file_name = open(os.environ["inputMessage"]).read()
    audio_file_name = 'english-2Minutes.wav'  # for local testing
    print "File name:", audio_file_name
    print "Fetching audio file from container..."
    audio_file = get_audio_file_from_container(audio_file_name)
    print "Splitting file to segments..."
    # audio_segments = split_audio_file_by_silence(audio_segment)
    time_interval_seconds = 10
    audio_segments = split_audio_file_by_time(audio_file, time_interval_seconds)
    print "Split {} seconds to {} segments of {} seconds".format(audio_file.duration_seconds, len(audio_segments), time_interval_seconds)
    print "Uploading segments to", audio_segments_container_name, "container..."
    upload_segments_to_container(audio_file_name, audio_segments, audio_segments_container_name)
    print "Done!"
