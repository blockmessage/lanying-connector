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

def mp3_to_silk(mp3_file, silk_file):
    pcm_file = silk_file+'.pcm'
    mp3_to_pcm(mp3_file, pcm_file)
    pilk.encode(pcm_file, silk_file, pcm_rate=44100, tencent=True)

def mp3_to_pcm(mp3_file, pcm_file):
    # 使用 PyDub 加载 MP3 文件
    sound = AudioSegment.from_file(mp3_file)
    # 将音频保存为 PCM 文件
    sound.export(pcm_file, format="s16le", parameters=["-ar", "44100", "-ac", "1"])

def get_duration(audio_filename):
    duration_ms = 0
    try:
        audio = AudioSegment.from_file(audio_filename)
        duration_ms = len(audio)
    except Exception as e:
        logging.exception(e)
    return round(duration_ms / 1000)
