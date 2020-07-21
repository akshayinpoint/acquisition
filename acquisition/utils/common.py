"""Utility for performing common functions."""

import logging
import os
import socket
from datetime import datetime
from typing import Optional, Union

import pytz


def check_internet(timeout: Union[float, int] = 10.0,
                   log: logging.Logger = None) -> bool:
  """Check the internet connectivity."""
  # You can find the reference code here:
  # https://gist.github.com/yasinkuyu/aa505c1f4bbb4016281d7167b8fa2fc2
  try:
    socket.create_connection(('www.google.com', 80), timeout=timeout)
    log.debug('Internet connection available.')
    return True
  except OSError:
    pass
  log.warning('Internet connection unavailable.')
  return False


def now() -> datetime:
  """Return current time without microseconds."""
  return datetime.now().replace(microsecond=0)


def utc_now() -> datetime:
  """Return UTC time without microseconds."""
  return datetime.now(pytz.utc).replace(microsecond=0)


def convert_bytes(number: Union[float, int]) -> Optional[str]:
  """Converts the number into size denominations.

  This function will convert bytes to KB, MB, GB, etc.

  Args:
    number: Number to be converted into file size.

  Returns:
    File size denomation.
  """
  for idx in ['bytes', 'KB', 'MB', 'GB', 'TB']:
    if number < 1024.0:
      return '%3.1f %s' % (number, idx)
    number /= 1024.0


def file_size(path: str) -> Optional[str]:
  """Return the file size."""
  if os.path.isfile(path):
    return convert_bytes(os.stat(path).st_size)


def timestamp_dirname(ts_format: str = '%d_%m_%Y_%H_%M_%S') -> str:
  """Returns current time in a timestamp format."""
  return str(now().strftime(ts_format))


def calculate_duration(start_time: str,
                       end_time: str,
                       timestamp_format: str = '%Y-%m-%d %H:%M:%S',
                       turntable_mode: bool = False
                       ) -> float:
  """Calculate the time duration in secs for selected intervals.

  Calculates the time delta between given intervals in secs.

  Args:
    start_time: Starting timestamp.
    end_time: Ending timestamp.
    timestamp_format: Timestamp format (default: %Y-%m-%d %H:%M:%S) of
                      the given intervals.

  Returns:
    Timedelta in secs.
  """
  if turntable_mode:
    _start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    _end_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
  else:
    _start_time = datetime.strptime(start_time, timestamp_format)
    _end_time = datetime.strptime(end_time, timestamp_format)
  return float((_end_time - _start_time).seconds)


def seconds_to_datetime(second: int) -> str:
  """Convert seconds to datetime string."""
  mins, secs = divmod(second, 60)
  hours, mins = divmod(mins, 60)
  return '%02d:%02d:%02d' % (hours, mins, secs)


def datetime_to_utc(timestamp: str,
                    timezone: str,
                    timestamp_format: str = '%Y-%m-%d %H:%M:%S') -> str:
  """Convert timezone specific timestamp to UTC time."""
  local = datetime.strptime(timestamp, timestamp_format)
  return str(pytz.timezone(timezone).localize(local).astimezone(pytz.utc))[:-6]
