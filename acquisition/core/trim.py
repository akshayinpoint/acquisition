"""A subservice for trimming the videos."""

import logging
import os
import random
import time
from datetime import datetime
from typing import List, Optional, Union

from moviepy.editor import VideoFileClip as vfc

from acquisition.utils.common import calculate_duration
from acquisition.utils.local import filename, quick_rename, temporary_copy

#TODO(xames3): Update docstrings to match the latest argument
# requirements.


def duration(file: str,
             for_humans: bool = False) -> Union[float, str, int]:
  """Returns duration of the video file."""
  if for_humans:
    mins, secs = divmod(vfc(file, audio=False).duration, 60)
    hours, mins = divmod(mins, 60)
    return '%02d:%02d:%02d' % (hours, mins, secs)
  else:
    return vfc(file, audio=False).duration


def trim_video(file: str,
               output: str,
               start: Union[float, int, str] = 0,
               end: Union[float, int, str] = 30) -> None:
  """Trims video.

  Trims video as per the requirements.
  Args:
    file: File to be used for trimming.
    output: Path of the output file.
    start: Starting point (default: 0) of the video in secs.
    end: Ending point (default: 30) of the video in secs.
    codec: Codec (default: libx264 -> .mp4) to be used while trimming.
    bitrate: Bitrate (default: min. 400) used while trimming.
    fps: FPS (default: 24) of the trimmed video clips.
    audio: Boolean (default: False) value to have audio in trimmed
            videos.
    preset: The speed (default: ultrafast) used for applying the
            compression technique on the trimmed videos.
    threads: Number of threads (default: 15) to be used for trimming.
  """
  video = vfc(file, audio=False, verbose=True).subclip(start, end)
  video.write_videofile(output, logger=None)
  video.close()
  try:
    del video
  except NameError as _nerr:
    print(_nerr)


def trim_num_parts(file: str,
                   num_parts: int,
                   equal_distribution: bool = False,
                   clip_length: Union[float, int, str] = 30,
                   random_start: bool = True,
                   random_sequence: bool = True) -> Optional[List]:
  """Trim video in number of equal parts.

  Trims the video as per the number of clips required.
  Args:
    file: File to be used for trimming.
    num_parts: Number of videos to be trimmed into.
    codec: Codec (default: libx264 -> .mp4) to be used while trimming.
    bitrate: Bitrate (default: min. 400) used while trimming.
    fps: FPS (default: 24) of the trimmed video clips.
    audio: Boolean (default: False) value to have audio in trimmed
            videos.
    preset: The speed (default: ultrafast) used for applying the
            compression technique on the trimmed videos.
    threads: Number of threads (default: 15) to be used for trimming.
    verbose: Boolean (default: False) value to display the status.
    return_list: Boolean (default: True) value to return list of all the
                 trimmed files.
  """
  num_parts = int(num_parts)
  clip_length = int(clip_length)
  split_part = duration(file) / num_parts
  start = 0
  # Start splitting the videos into 'num_parts' equal parts.
  video_list = []
  for idx in range(1, num_parts + 1):
    start, end = start, start + split_part
    trim_video(file, filename(file, idx), start, end)
    start += split_part
    video_list.append(filename(file, idx))
  if equal_distribution:
    for file in video_list:
      if clip_length <= split_part:
        start, end = 0, clip_length
        if random_start:
          start = random.randint(1, int(duration(file)))
          end = start + clip_length
        file, temp = quick_rename(file)
        trim_video(temp, file, start, end)
        time.sleep(2.0)
  if random_sequence:
    return random.shuffle(video_list)
  else:
    return video_list


def trim_sample_section(file: str,
                        sampling_rate: Union[float, int, str]) -> None:
  """Trim a sample portion of the video as per the sampling rate.

  Trims a random sample portion of the video as per the sampling rate.
  Args:
    file: File to be used for trimming.
    sampling_rate: Portion of the video to be trimmed.
    codec: Codec (default: libx264 -> .mp4) to be used while trimming.
    bitrate: Bitrate (default: min. 400) used while trimming.
    fps: FPS (default: 24) of the trimmed video.
    audio: Boolean (default: False) value to have audio in trimmed
            video.
    preset: The speed (default: ultrafast) used for applying the
            compression technique on the trimmed video.
    threads: Number of threads (default: 15) to be used for trimming.
  Returns:
    Path of the temporary duplicate file created.
  """
  sampling_rate = float(sampling_rate)
  temp = temporary_copy(file)

  clip_length = int((duration(file) * sampling_rate * 0.01))
  start = random.randint(1, int(duration(file) - clip_length))
  end = start + clip_length
  trim_video(temp, file, start, end)
  os.remove(temp)


def trim_by_factor(file: str,
                   factor: str = 's',
                   clip_length: Union[float, int, str] = 30,
                   last_clip: bool = True) -> List:
  """Trims the video by deciding factor.

  Trims the video as per the deciding factor i.e. trim by mins OR trim
  by secs.
  Args:
    file: File to be used for trimming.
    factor: Trimming factor (default: secs -> s) to consider.
    clip_length: Length (default: 30) of each video clip.
    last_clip: Boolean (default: True) value to consider the remaining
               portion of the trimmed video.
    codec: Codec (default: libx264 -> .mp4) to be used while trimming.
    bitrate: Bitrate (default: min. 400) used while trimming.
    fps: FPS (default: 24) of the trimmed video clips.
    audio: Boolean (default: False) value to have audio in trimmed
            videos.
    preset: The speed (default: ultrafast) used for applying the
            compression technique on the trimmed videos.
    threads: Number of threads (default: 15) to be used for trimming.
  """
  clip_length = int(clip_length)
  total_length = duration(file)
  video_list = []
  idx = 1
  if factor == 'm':
    start, end, clip_length = 0, clip_length * 60, clip_length * 60
  else:
    start, end = 0, clip_length
  while clip_length < total_length:
    trim_video(file, filename(file, idx), start, end)
    video_list.append(filename(file, idx))
    start, end, idx = end, end + clip_length, idx + 1
    total_length -= clip_length
  else:
    if last_clip:
      start, end = (duration(file) - total_length), duration(file)
      trim_video(file, filename(file, idx), start, end)
      video_list.append(filename(file, idx))
  return video_list


def trim_sub_sample(file: str,
                    start_time: str,
                    end_time: str,
                    sample_start_time: str,
                    sample_end_time: str,
                    timestamp_format: str = '%H:%M:%S') -> str:
  """Trims sample of the video based on provided timestamp."""
  trim_duration = calculate_duration(sample_start_time, sample_end_time)
  _start_time = datetime.strptime(start_time, timestamp_format)
  _start_time = int(_start_time.strftime('%s'))
  _sample_start_time = datetime.strptime(sample_start_time, timestamp_format)
  _sample_start_time = int(_sample_start_time.strftime('%s'))
  _end_time = datetime.strptime(end_time, timestamp_format)
  _end_time = int(_end_time.strftime('%s'))
  _sample_end_time = datetime.strptime(sample_end_time, timestamp_format)
  _sample_end_time = int(_sample_end_time.strftime('%s'))
  idx = 1
  if duration(file) < trim_duration:
    trim_duration = duration(file)
  if _sample_start_time < _start_time:
    start = 0
  else:
    start = int(_sample_start_time - _start_time)
  if _sample_end_time < _end_time:
    end = int(start + trim_duration)
  else:
    end = duration(file)
  trim_video(file, filename(file, idx), start, end)
  return filename(file, idx)


def trim_by_points(file: str,
                   start_time: int,
                   end_time: int,
                   factor: str = 's') -> str:
  """Trim by starting minute OR starting seconds."""
  idx = 1
  start_time = int(start_time)
  end_time = int(end_time)

  _factor = 1 if factor == 's' else 60
  total_limit = int(duration(file) / _factor)

  if factor == 'p':
    start_time = int((start_time / 100) * total_limit)
    end_time = int((end_time / 100) * total_limit)
    total_limit = 100

  if end_time < start_time:
    raise Exception('Ending time is less than starting time.')
  else:
    if end_time >= total_limit:
      if factor == 'p':
        print('Video doesn\'t have frame to process.')
      else:
        print('Video doesn\'t have frames to process and will max out.')
      end_time = total_limit
    elif start_time < 0:
      print('Start should be greater than 0.')
      start_time = 0
    trim_video(file, filename(file, idx), start_time * _factor,
               end_time * _factor)
  return filename(file, idx)
