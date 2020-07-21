"""Utility for making convenient use of OpenCV."""

import logging
import socket
from typing import Any, Optional, Union

import cv2
import numpy as np


def rescale(frame: np.ndarray,
            width: Optional[int] = 300,
            height: Optional[int] = None,
            interpolation: Optional[Any] = cv2.INTER_AREA) -> np.ndarray:
  """Rescale the frame.

  Rescale the stream to a desirable size. This is required before
  performing the necessary operations.

  Args:
    frame: Numpy array of the image frame.
    width: Width (default: None) to be rescaled to.
    height: Height (default: None) to be rescaled to.
    interpolation: Interpolation algorithm (default: INTER_AREA) to be
                    used.

  Returns:
    Rescaled numpy array for the input frame.
  """
  dimensions = None
  frame_height, frame_width = frame.shape[:2]
  # If both width & height are None, then return original frame size.
  # No rescaling will be done in that case.
  if width is None and height is None:
    return frame

  if width and height:
    dimensions = (width, height)
  elif width is None:
    ratio = height / float(frame_height)
    dimensions = (int(frame_width * ratio), height)
  else:
    ratio = width / float(frame_width)
    dimensions = (width, int(frame_height * ratio))

  return cv2.resize(frame, dimensions, interpolation=interpolation)


def disconnect(stream: np.ndarray) -> None:
  """Disconnect stream and exit the program."""
  stream.release()
  cv2.destroyAllWindows()


def configure_camera_url(camera_address: str,
                         camera_username: str = 'admin',
                         camera_password: str = 'iamironman',
                         camera_port: int = 554,
                         camera_stream_address: str = 'H.264',
                         camera_protocol: str = 'rtsp') -> str:
  """Configure camera url for testing."""
  return (f'{camera_protocol}://{camera_username}:{camera_password}@'
          f'{camera_address}:{camera_port}/{camera_stream_address}')


def camera_live(camera_address: str,
                camera_port: Union[int, str],
                log: logging.Logger,
                timeout: Union[float, int, str] = 10.0) -> bool:
  """Check if any camera connectivity is available."""
  # You can find the reference code here:
  # https://gist.github.com/yasinkuyu/aa505c1f4bbb4016281d7167b8fa2fc2
  try:
    timeout = float(timeout)
    camera_port = int(camera_port)
    socket.create_connection((camera_address, camera_port), timeout = timeout)
    log.info('Camera connected to the network.')
    return True
  except OSError:
    pass
  log.warning('Camera not connected to any network.')
  return False
