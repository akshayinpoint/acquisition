"""Utility for replacing prints with logs which make sense."""

import logging
import os
import sys
import time
from pathlib import Path

from acquisition.utils.paths import logs


class TimeFormatter(logging.Formatter):

  def formatTime(self, record, datefmt=None):
    convert = self.converter(record.created)
    if datefmt:
      if '%F' in datefmt:
        msec = '%03d' % record.msecs
        datefmt = datefmt.replace('%F', msec)
      string = time.strftime(datefmt, convert)
    else:
      temp = time.strftime('%Y-%m-%d %H:%M:%S', convert)
      string = '%s.%03d' % (temp, record.msecs)
    return string


def log(level: str = 'debug') -> logging.Logger:
  """Create log file and log print events.

  Args:
    file: Current file name.
    level: Logging level.

  Returns:
    Logger object which records logs in ./logs/ directory.
  """
  logger = logging.getLogger()
  logger.setLevel(f'{level.upper()}')
  name = Path(os.path.abspath(sys.modules["__main__"].__file__)).stem
  name = f'{name}.log'
  name = Path(os.path.join(logs, name))
  custom_format = ('%(asctime)s    %(levelname)-8s    %(threadName)-8s    '
                   '%(filename)s:%(lineno)-15s    %(message)s')
  formatter = TimeFormatter(custom_format, '%Y-%m-%d %H:%M:%S.%F %Z')
  # Create log file.
  file_handler = logging.FileHandler(os.path.join(logs, name))
  file_handler.setFormatter(formatter)
  logger.addHandler(file_handler)
  # Print log statement.
  stream_handler = logging.StreamHandler(sys.stdout)
  stream_handler.setFormatter(formatter)
  logger.addHandler(stream_handler)
  # Return logger object.
  return logger
