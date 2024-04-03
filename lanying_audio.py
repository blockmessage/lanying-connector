import pilk
from pydub import AudioSegment
import logging
import subprocess

def silk_to_mp3(input_file, output_file):
    pcm_file = output_file+".pcm"
    pilk.decode(input_file, pcm_file, pcm_rate=44100)
    pcm_to_mp3(pcm_file, output_file)

def pcm_to_mp3(pcm_file, mp3_file):
    sound = AudioSegment.from_file(pcm_file, format="raw", frame_rate=44100, channels=1, sample_width=2)
    # 将音频保存为 MP3 文件
    sound.export(mp3_file, format="mp3", bitrate="192k")

def get_duration(audio_filename):
    duration_ms = 0
    try:
        audio = AudioSegment.from_file(audio_filename)
        duration_ms = len(audio)
    except Exception as e:
        logging.exception(e)
    return duration_ms
