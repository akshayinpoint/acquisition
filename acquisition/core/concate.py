"""A subservice for concatenating the videos."""

import os
from typing import Optional

from acquisition.core.trim import duration as drn
from acquisition.utils.boto_wrap import video_file_extensions
from acquisition.utils.common import file_size, timestamp_dirname


def concate_videos(directory: str,
                   delete_old_files: bool = True) -> Optional[str]:
  """Concatenates video.

  Concatenates videos as per the requirements.

  Args:
    file: List of files to be concatenated.
    output: Name of the output file.
    delete_old_files: Boolean (default: True) value to delete the older
                      files once the concatenation is done.

  Returns:
    Path where the concatenated file is created.
  """
  files = [os.path.join(directory, file) for file in os.listdir(directory)
           if file_size(os.path.join(directory, file)) != '300.0 bytes']
  files.sort(key=os.path.getctime)
  files = [f"file '{file}'\n" for file in files
           if file.endswith(video_file_extensions)]
  if len(os.listdir(directory)) == 0:
    return None
  if len(os.listdir(directory)) == 1:
    if drn(os.path.join(directory, os.listdir(directory)[0])) == '300.0 bytes':
      os.remove(os.path.join(directory, os.listdir(directory)[0]))
      return None
    return os.path.join(directory, os.listdir(directory)[0])
  temp_file_xa = os.path.join(directory, f'{timestamp_dirname()}.tmp_xa')
  with open(temp_file_xa, 'w') as file:
    file.writelines(files)
  output = os.path.join(directory, f'{timestamp_dirname()}.mp4')
  os.system(f'ffmpeg -loglevel error -y -f concat -safe 0 -i '
            f'{os.path.join(directory, f"{timestamp_dirname()}.tmp_xa")} '
            f'-vcodec copy -acodec copy {output}')
  if delete_old_files:
    temp = [os.path.join(directory, file) for file in os.listdir(directory)
            if os.path.join(directory, file).endswith(video_file_extensions)]
    temp.append(temp_file_xa)
    try:
      temp.remove(output)
    except ValueError:
      pass
    for file in temp:
      os.remove(file)
  return output
