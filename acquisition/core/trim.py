"""A subservice for trimming the videos."""

import os
import random
from typing import List, Union

from moviepy.editor import VideoFileClip as vfc

from acquisition.utils.local import filename, temporary_copy

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

      if duration(filename(file, idx)) > (0.7 * clip_length):
        video_list.append(filename(file, idx))

  return video_list
