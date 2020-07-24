"""A subservice for storing live video over camera."""

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Union

from acquisition.core.concate import concate_videos
from acquisition.core.trim import duration as drn
from acquisition.utils.boto_wrap import access_file
from acquisition.utils.common import (calculate_duration, datetime_to_utc,
                                      file_size, now, seconds_to_datetime)
from acquisition.utils.fetch import (batch_download_from_ftp,
                                     download_from_azure,
                                     download_from_google_drive,
                                     earthcam_specific_download)
from acquisition.utils.generate import video_type
from acquisition.utils.local import filename
from acquisition.utils.opencvapi import camera_live, configure_camera_url
from acquisition.utils.paths import videos


def ffmpeg_str(source: str,
               file_name: str,
               duration: Union[timedelta, float, int, str],
               camera_timeout: Union[float, int, str] = 30.0) -> str:
  """Returns FFMPEG's main command to run using subprocess module.

  Returns FFMPEG's custom command for recording the live feed & storing
  it in a file for further processing.

  Args:
    source: RTSP camera url.
    file_name: Path where you need to save the output file.
    duration: Duration in secs that needs to be captured by FFMPEG.
    camera_timeout: Maximum time to wait until disconnection occurs.

  Returns:
    FFMPEG compatible & capable string for video recording over RTSP. 
  """
  timeout = float(camera_timeout)
  ffmpeg = 'ffmpeg.exe' if os.name == 'nt' else 'ffmpeg'
  return (f'{ffmpeg} -loglevel error -y -rtsp_transport tcp -i {source} '
          f'-vcodec copy -acodec copy -t {duration} -vcodec libx264 '
          f'{file_name} -timeout {timeout}')


def live(bucket_name: str,
         order_name: str,
         run_date: str,
         start_time: str,
         end_time: str,
         camera_address: str,
         camera_username: str,
         camera_password: str,
         camera_port: Union[int, str],
         camera_timeout: Union[float, int, str],
         log: logging.Logger,
         timestamp_format: str = '%H:%M:%S') -> Optional[str]:
  """Record live videos based on time duration using FFMPEG.

  Args:
    bucket_name: S3 bucket name.
    order_name: Order name.
    run_date: Date when to record the video.
    start_time: Time when to start recording the video.
    end_time: Time when to stop recording the video.
    camera_address: Camera's IP address.
    camera_username: Camera username.
    camera_password: Camera password.
    camera_port: Camera port number.
    camera_timeout: Maximum time to wait until disconnection occurs.
    timestamp_format: Timestamp for checking the recording start time.
    log: Logger object.
  """
  camera_port = int(camera_port)
  camera_timeout = float(camera_timeout)

  start_time, end_time = f'{run_date} {start_time}', f'{run_date} {end_time}'
  duration = calculate_duration(start_time, end_time, timestamp_format, True)
  force_close = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
  force_close = force_close.replace(tzinfo=timezone.utc).timestamp()

  vid_type = video_type(True, True, True)
  temp = os.path.join(videos, f'{bucket_name}{order_name}')

  if not os.path.isdir(temp):
    os.mkdir(temp)
  temp_file = os.path.join(temp, f'{bucket_name}{order_name}{vid_type}.mp4')

  url = configure_camera_url(camera_address, camera_username,
                             camera_password, camera_port)
  slept_duration, idx = 0, 1

  if duration != 0:
    try:
      while True:
        if camera_live(camera_address, camera_port, log, camera_timeout):
          file = filename(temp_file, idx)
          log.info('Recording started for selected camera.')
          os.system(ffmpeg_str(url, file, duration, camera_timeout))

          stop_utc = now().replace(tzinfo=timezone.utc).timestamp()
          stop_secs = now().second

          _old_file = file_size(file)
          old_duration = stop_secs if _old_file == '300.0 bytes' else drn(file)
          duration = duration - old_duration - slept_duration

          slept_duration = 0
          idx += 1
          if (force_close <= stop_utc) or (duration <= 0):
            output = concate_videos(temp, delete_old_files=True)
            if output:
              return output
        else:
          log.warning('Unable to record because of poor network connectivity.')
          slept_duration += camera_timeout
          log.warning('Compensating lost time & attempting after 30 secs.')
          time.sleep(camera_timeout)
    except Exception as error:
      log.critical(f'Something went wrong because of {error}')


def stored(json_data: dict, log: logging.Logger):
  try:
    _status, _file = None, None
    scheduled = json_data.get('schedule_download', False)
    if scheduled:
      scheduled_time = f'{json_data["start_date"]} {json_data["start_time"]}:00'
      sleep_interval = datetime_to_utc(scheduled_time,
                                       json_data["camera_timezone"],
                                       '%Y-%m-%d %H:%M:%S')
      sleep_interval = datetime.strptime(sleep_interval, '%Y-%m-%d %H:%M:%S')
      sleep_interval -= now()
      if sleep_interval.seconds <= 0:
        log.error('Scheduled time has passed already.')
        return None
      log.info('Video is scheduled for downloading, the process will suspend '
               f'for {seconds_to_datetime(int(sleep_interval.seconds))}.')
      time.sleep(1.0 + sleep_interval.seconds)
    log.info('Initiating video download...')
    if json_data.get('access_type', None) == 'GCP':
      log.info('Downloading file via Google Drive...')
      _status, _file = download_from_google_drive(json_data['g_url'],
                                                  json_data['stored_filename'], log)
    elif json_data.get('access_type', None) == 'Microsoft':
      log.info('Downloading file via Microsoft Azure...')
      _status, _file = download_from_azure(json_data['azure_account_name'],
                                           json_data['azure_account_key'],
                                           json_data['azure_container_name'],
                                           json_data['azure_blob_name'],
                                           json_data['stored_filename'], log)
    elif json_data.get('access_type', None) == 'FTP':
      log.info('Downloading file via FTP...')
      if json_data.get("earthcam_download", False):
        log.info('Downloading EarthCam file(s)...')
        _status, _file = earthcam_specific_download(json_data['p_name'],
                                   json_data['p_pass'],
                                   json_data['p_ip'],
                                   json_data['point_access'],
                                   json_data['earthcam_start_date'],
                                   json_data['earthcam_start_time'],
                                   json_data['earthcam_end_time'],
                                   log, json_data['stored_filename'])
      else:
         _status, _file = batch_download_from_ftp(json_data['p_name'],
                                                  json_data['p_pass'],
                                                  json_data['p_ip'],
                                                  json_data['point_access'], log)
    elif json_data.get('access_type', None) == 'S3':
      log.info('Downloading file via Amazon S3 storage...')
      _status, _file = access_file(json_data['s3_access_key'],
                                          json_data['s3_secret_key'],
                                          json_data['s3_url'],
                                          json_data['stored_filename'], log,
                                          json_data['s3_bucket_name'])
    return _status, _file
  except Exception as error:
    log.exception(error)
