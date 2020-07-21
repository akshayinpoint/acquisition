"""Utility to fetch files from Google Drive, Azure and FTP."""

import csv
import ftplib
import glob
import logging
import os
import re
import shutil
from datetime import datetime, timedelta
from typing import List, Optional, Tuple, Union
from urllib.parse import unquote, urlsplit
from uuid import uuid4

import pytz
import requests
from azure.storage.blob import BlobClient, ContainerClient
from requests.exceptions import RequestException
from urllib3.exceptions import RequestError

from acquisition.core.concate import concate_videos
from acquisition.core.trim import trim_by_factor
from acquisition.utils.boto_wrap import (access_limited_files,
                                         upload_to_bucket,
                                         video_file_extensions)
from acquisition.utils.common import file_size
from acquisition.utils.paths import videos

# pyright: reportInvalidStringEscapeSequence=false
check_file = re.compile("^(\/+\w{0,}){0,}\.\w{1,}$")
check_directory = re.compile("^(\/+\w{0,}){0,}$")

# This url is used for downloading files from Google Drive.
DRIVE_DOWNLOAD_URL = 'https://docs.google.com/uc?export=download'
# Chunk size
CHUNK_SIZE = 32768
# Credentials
_AWS_ACCESS_KEY = 'XAMES3'
_AWS_SECRET_KEY = 'XAMES3'

def filename_from_url(public_url: str) -> str:
  """Returns filename from public url.

  Args:
    public_url: Url of the file.

  Returns:
    Extracted filename from it's url.

  Raises:
    ValueError: If the url has arbitrary characters.
  """
  url_path = urlsplit(public_url).path
  basename = os.path.basename(unquote(url_path))
  if (os.path.basename(basename) != basename
      or unquote(os.path.basename(url_path)) != basename):
    raise ValueError('[e] URL has invalid characters. Cannot parse the same.')
  return basename


def download_from_url(public_url: str,
                      filename: str = None,
                      download_path: str = videos) -> Tuple:
  """Downloads file from the url.

  Downloads file from the url and saves it in videos folder.

  Args:
    public_url: Url of the file.
    filename: Filename (default: None) for the downloaded file.
    download_path: Path (default: ./videos/) for saving file.

  Returns:
    Boolean value if the file is downloaded or not.

  Raises:
    RequestError: If any error occurs while making an HTTP request.
    RequestException: If any exception occurs if `requests` related
    exceptions occur.

  Notes:
    This function is tested on AWS S3 public urls and can download the
    same. This function doesn't work if tried on google drive. For
    accessing/downloading files over Google Drive, please use -
    `download_from_google_drive()`.
  """
  try:
    download_item = requests.get(public_url, stream=True)
    if filename is None:
      filename = filename_from_url(public_url)
    with open(os.path.join(download_path, filename), 'wb') as file:
      file.write(download_item.content)
      return True, os.path.join(download_path, filename)
  except (RequestError, RequestException):
    return None, '[e] Error while downloading file'


def fetch_confirm_token(response: requests.Response):
  """Don't know what this is, hence docstring not updated yet."""
  # TODO(xames3): Update the docstring accordingly.
  for k, v in response.cookies.items():
    if k.startswith('download_warning'):
      return v
  else:
    return None


def download_from_google_drive(shareable_url: str,
                               file_name: str,
                               log: logging.Logger,
                               download_path: str = videos) -> Tuple:
  """Downloads file from the shareable url.

  Downloads file from shareable url and saves it in videos folder.

  Args:
    shareable_url: Url of the file.
    file_name: Filename for the downloaded file.
    log: Logger object for logging the status.
    download_path: Path (default: ./videos/) for saving file.

  Returns:
    Boolean value if the file is downloaded or not.

  Raises:
    ResponseError: If any unexpected response/error occurs.
    ResponseNotChunked: If the response is not sending correct `chunks`.

  Notes:
    This function is capable of downloading files from Google Drive iff
    these files are shareable using 'Anyone with the link' link sharing
    option.
  """
  # You can find the reference code here:
  # https://stackoverflow.com/a/39225272
  try:
    if ('/view') in shareable_url:
      file_id = shareable_url.split('file/d/')[1].split('/view')[0]
    else:
      file_id = shareable_url.split('https://drive.google.com/open?id=')[1]

    session = requests.Session()
    response = session.get(DRIVE_DOWNLOAD_URL,
                           params={'id': file_id},
                           stream=True)
    token = fetch_confirm_token(response)
    if token:
      response = session.get(DRIVE_DOWNLOAD_URL,
                             params={'id': file_id, 'confirm': token},
                             stream=True)
    # Write file to the disk.
    with open(os.path.join(download_path, f'{file_name}.mp4'), 'wb') as file:
      for chunk in response.iter_content(CHUNK_SIZE):
        if chunk:
          file.write(chunk)
    log.info(f'File "{file_name}.mp4" downloaded from Google Drive.')
    if file_size(os.path.join(download_path, f'{file_name}.mp4')).endswith('KB'):
      log.error('Unusable file downloaded since file size is in KBs.')
      return None, '[w] Unusable file downloaded.'
    return True, os.path.join(download_path, f'{file_name}.mp4')
  except Exception as error:
    log.exception(error)
    log.error('File download from Google Drive failed because of poor network '
              'connectivity.')
    return None, '[e] Error while downloading file'


def generate_connection_string(account_name: str,
                               account_key: str,
                               protocol: str = 'https') -> str:
  """Generates the connection string for Microsoft Azure."""
  connection_string = (f'DefaultEndpointsProtocol={protocol};'
                       f'AccountName={account_name};AccountKey={account_key};'
                       'EndpointSuffix=core.windows.net')
  return connection_string


def download_from_azure(account_name: str,
                        account_key: str,
                        container_name: str,
                        blob_name: str,
                        file_name: str,
                        log: logging.Logger,
                        download_path: str = videos) -> Tuple:
  """Download file from Microsoft Azure.

  Download file from Microsoft Azure and store it in videos folder.

  Args:
    account_name: Azure account name.
    account_key: Azure account key.
    container_name: Container from which blob needs to be downloaded.
    blob_name: Blob to download from Microsoft Azure.
    file_name: Filename for the downloaded file.
    log: Logger object for logging the status.
    download_path: Path (default: ./videos/) for saving file.

  Returns:
    Boolean value if the file is downloaded or not.
  """
  # You can find the reference code here:
  # https://pypi.org/project/azure-storage-blob/
  try:
    connection_string = generate_connection_string(account_name, account_key)
    blob = BlobClient.from_connection_string(conn_str=connection_string,
                                             container_name=container_name,
                                             blob_name=blob_name)
    with open(os.path.join(download_path, f'{file_name}.mp4'), 'wb') as file:
      data = blob.download_blob()
      data.readinto(file)
    log.info(f'File "{file_name}.mp4" downloaded from Microsoft Azure.')
    if file_size(os.path.join(download_path, f'{file_name}.mp4')).endswith('KB'):
      log.error('Unusable file downloaded since file size is in KBs.')
      return None, '[w] Unusable file downloaded.'
    return True, os.path.join(download_path, f'{file_name}.mp4')
  except Exception:
    log.error('File download from Microsoft Azure failed because of poor '
              'network connectivity.')
    return None, '[e] Error while downloading file'


def get_blob_url(account_name: str,
                 container_name: str,
                 blob_name: str) -> str:
  """Get blob URL."""
  return (f'https://{account_name}.blob.core.windows.net/{container_name}'
          f'/{blob_name}')


def download_using_ftp(username: str,
                       password: str,
                       public_address: str,
                       remote_file: str,
                       log: logging.Logger,
                       download_path: str = videos) -> Tuple:
  """Download/fetch/transfer file using OpenSSH via FTP.

  Fetch file from remote machine to store it in videos folder.

  Args:
    username: Username of the remote machine.
    password: Password of the remote machine.
    public_address: Remote server IP address.
    remote_file: Remote file to be downloaded/transferred.
    file_name: Filename for the downloaded file.
    log: Logger object for logging the status.
    download_path: Path (default: ./videos/) for saving file.

  Returns:
    Boolean value if the file is downloaded or not.
  """
  # You can find the reference code here:
  # https://stackoverflow.com/a/56850195
  status = False
  file_name = 'xa.mp4'

  parts = public_address.split('.')
  addr_type = len(parts) == 4 and all(0 <= int(part) < 256 for part in parts)

  try:
    if remote_file.endswith(video_file_extensions):
      if addr_type:
        os.system(f'sshpass -p {password} scp -o StrictHostKeyChecking=no '
                  f'{username}@{public_address}:{remote_file} {download_path}')
        if remote_file.startswith('/'):
          remote_file = remote_file[1:]

        file_name = os.path.join(download_path, os.path.basename(remote_file))
      else:
        ftp = ftplib.FTP(public_address)
        ftp.login(username, password)
        ftp.cwd(os.path.dirname(remote_file))

        filename = os.path.basename(remote_file)
        file_name = os.path.join(download_path, filename)
        ftp.retrbinary(f'RETR {filename}', open(file_name, 'wb').write)

        ftp.quit()
      status = True
    else:
      return None, 'Remote file is not a media file.'
  except OSError:
    log.error('File transfer via FTP failed because of poor network '
              'connectivity.')
    return None, '[e] Error while transferring file'
  finally:
    if status:
      if file_size(file_name).endswith('KB'):
        print(file_name)
        log.error('Unusable file transferred since file size is in KBs.')
        return None, '[w] Unusable file transferred.'
      else:
        log.info(f'File "{os.path.basename(file_name)}" transferred '
                 'successfully')
        return True, file_name
    else:
      return None, '[e] Error while transferring file'


def concate_batch_from_s3(access_key: str,
                          secret_key: str,
                          bucket_name: str,
                          access_from: str,
                          access_to: str,
                          log: logging.Logger,
                          trim_hrs: Optional[Union[float, int]] = None,
                          timestamp_format: str = '%Y-%m-%d %H:%M:%S') -> List:
  """Downloads multiple files from S3 and concatenate them.

  Download multiple files from S3 bucket for particular timeframe and
  concatenate them resulting into a single file in each directory.

  Args:
    access_key: AWS access key.
    secret_key: AWS saccess_key: str,
    bucket_name: Bucket to search and download from.
    access_from: Datetime from when to start fetching files.
    access_to: Datetime till when to fetch files.
    log: Logger object for logging the status.
    timestamp_format: Timestamp format (default: %Y-%m-%d %H:%M:%S)

  Returns:
    List of the concatenated files.
  """
  log.info(f'Downloading files from "{bucket_name}" for range {access_from} '
           f'to {access_to} from Amazon S3.')
  list_of_dirs = access_limited_files(access_key, secret_key, bucket_name,
                                      access_from, access_to, log,
                                      timestamp_format)
  sorted(list_of_dirs)
  if len(list_of_dirs) > 0:
    log.info('Concatenating files in their subsequent directories.')
    temp = [concate_videos(idx) for idx in list_of_dirs]
    if trim_hrs:
      log.info(f'Trimming concatenated videos in batches of {trim_hrs} hrs.')
      # pyright: reportGeneralTypeIssues=false
      return [trim_by_factor(c_idx, 'm', (trim_hrs * 60)) for c_idx in temp]
    return temp
  else:
    log.warning('0 files downloaded. Returning empty list.')
    return []


def batch_download_from_azure(account_name: str,
                              account_key: str,
                              container_name: str,
                              access_from: str,
                              access_to: str,
                              log: logging.Logger,
                              timestamp_format: str = '%Y-%m-%d %H:%M:%S',
                              download_path: str = videos) -> List:
  """Download multiple files from Microsoft Azure.

  Download multiple files from Azure Blob container for particular
  timeframe.

  Args:
    account_name: Azure account name.
    account_key: Azure account key.
    container_name: Container from which blob needs to be downloaded.
    blob_name: Blob to download from Microsoft Azure.
    file_name: Filename for the downloaded file.
    log: Logger object for logging the status.
    download_path: Path (default: ./videos/) for saving file.

  Returns:
    List of the directories which hosts the downloaded files.
  """
  _glob = []
  # You can find the reference code here:
  # https://pypi.org/project/azure-storage-blob/
  try:
    connection_string = generate_connection_string(account_name, account_key)
    container = ContainerClient.from_connection_string(connection_string,
                                                  container_name=container_name)
    limit_from = datetime.strptime(
        access_from, timestamp_format).replace(tzinfo=pytz.UTC)
    limit_till = datetime.strptime(
        access_to, timestamp_format).replace(tzinfo=pytz.UTC)
    container_dir = os.path.join(videos, container_name)
    concate_dir = []
    files_with_timestamp = {}
    blobs_list = container.list_blobs()
    unsup_list = container.list_blobs()
    unsupported = [idx.name
                   for idx in unsup_list
                   if not (idx.name).endswith(video_file_extensions)]
    unsupported = list(set(map(lambda x: os.path.splitext(x)[1], unsupported)))
    unsupported = [idx for idx in unsupported if idx is not '']
    if len(unsupported) > 1:
      log.info(f'Unsupported video formats like "{unsupported[0]}", '
               f'"{unsupported[1]}", etc. will be skipped.')
    else:
      log.info(f'Files ending with "{unsupported[0]}" will be skipped.')
    for blob in blobs_list:
      if (blob.name).endswith(video_file_extensions):
        files_with_timestamp[blob.name] = blob.creation_time
    sorted_files = sorted(files_with_timestamp.items(), key=lambda xa: xa[1])
    for file, timestamp in sorted_files:
      if timestamp > limit_from and timestamp < limit_till:
        blob_style_dir = os.path.join(container_dir, os.path.dirname(file))
        concate_dir.append(blob_style_dir)
        if not os.path.isdir(blob_style_dir):
          os.makedirs(blob_style_dir)
        download_from_azure(account_name, account_key, container_name,
                            file, os.path.basename(file[:-4]), log,
                            blob_style_dir)
        _glob.append(os.path.join(blob_style_dir, os.path.basename(file)))
    if len(concate_dir) > 0:
      sizes = [file_size(s_idx) for s_idx in _glob]
      temp = [(n, s) for n, s in zip(_glob, sizes)]
      with open(os.path.join(container_dir, f'{container_name}.csv'), 'a',
                encoding="utf-8") as csv_file:
        log.info('Logging downloaded files into a CSV file.')
        _file = csv.writer(csv_file, quoting=csv.QUOTE_MINIMAL)
        _file.writerow(['Files', 'Size on disk'])
        _file.writerows(temp)
      return list(set(concate_dir))
    else:
      return []
  except Exception as e:
    log.exception(e)
    log.error('File download from Microsoft Azure failed because of poor '
              'network connectivity.')
    return []


def concate_batch_from_azure(account_name: str,
                             account_key: str,
                             container_name: str,
                             access_from: str,
                             access_to: str,
                             log: logging.Logger,
                             trim_hrs: Optional[Union[float, int]] = None,
                             timestamp_format: str = '%Y-%m-%d %H:%M:%S'
                             ) -> List:
  """Downloads multiple files from Azure and concatenate them.

  Download multiple files from Azure bucket for particular timeframe and
  concatenate them resulting into a single file in each directory.

  Args:
    account_name: Azure account name.
    account_key: Azure account key.
    container_name: Container from which blob needs to be downloaded.
    access_from: Datetime from when to start fetching files.
    access_to: Datetime till when to fetch files.
    log: Logger object for logging the status.
    timestamp_format: Timestamp format (default: %Y-%m-%d %H:%M:%S)

  Returns:
    List of the concatenated files.
  """
  log.info(f'Downloading files "{container_name}" for range {access_from} '
           f'to {access_to} from Microsoft Azure.')
  list_of_dirs = batch_download_from_azure(account_name, account_key,
                                           container_name, access_from,
                                           access_to, log, timestamp_format)
  sorted(list_of_dirs)
  if len(list_of_dirs) > 0:
    log.info('Concatenating files in their subsequent directories.')
    temp = [concate_videos(idx) for idx in list_of_dirs]
    if trim_hrs:
      log.info(f'Trimming concatenated videos in batches of {trim_hrs} hrs.')
      return [trim_by_factor(c_idx, 'm', (trim_hrs * 60)) for c_idx in temp]
    return temp
  else:
    log.warning('0 files downloaded. Returning empty list.')
    return []


def _is_ftp_dir(ftp: ftplib.FTP, name: str,
                guess_by_extension: bool = True) -> bool:
  """Simply determines if an item listed on the ftp server is a valid
  directory or not."""
  if guess_by_extension is True:
    if len(name) >= 4:
      if name[-4] == '.':
        return False

  original_cwd = ftp.pwd()

  try:
    ftp.cwd(name)
    ftp.cwd(original_cwd)
    return True
  except ftplib.error_perm as e:
    return False
  except Exception as e:
    return False


def _make_parent_dir(fpath: str, log: logging.Logger) -> None:
  """Ensures the parent directory of a filepath exists."""
  dirname = os.path.dirname(fpath)

  while not os.path.exists(dirname):
    try:
      os.makedirs(dirname)
      log.info(f'Created directory "{dirname}".')
    except OSError:
      _make_parent_dir(dirname, log)


def _download_ftp_file(ftp: ftplib.FTP, file_name: str,
                       remote_path: str, overwrite: bool,
                       log: logging.Logger) -> None:
  """Downloads a single file from an ftp server."""
  _make_parent_dir(remote_path.lstrip('/'), log)

  if not os.path.exists(remote_path) or overwrite is True:
    try:
      if file_name.endswith(video_file_extensions):
        ftp.retrbinary(f'RETR {file_name}', open(remote_path, 'wb').write)
        log.info(f'File "{os.path.basename(file_name)}" transferred '
                'successfully.')
      else:
        log.warning('Skipping non-media file(s).')
    except FileNotFoundError:
      log.error(f'File transferred failed for "{os.path.basename(file_name)}".')
  else:
    log.warning(f'File "{remote_path}" already exists.')


def _file_name_match_patern(pattern: str, file_name: str) -> bool:
  """Returns True if filename matches the pattern."""
  if pattern is None:
    return True
  else:
    return bool(re.match(pattern, file_name))


def _mirror_ftp_dir(ftp: ftplib.FTP, file_name: str, overwrite: bool,
                    guess_by_extension: bool, pattern: Union[None, str],
                    log: logging.Logger) -> None:
  """Replicates a remote directory on an ftp server recursively."""
  if pattern is None:
        pattern = ''

  for item in ftp.nlst(file_name):
    if _is_ftp_dir(ftp, item, guess_by_extension):
      _mirror_ftp_dir(ftp, item, overwrite, guess_by_extension, pattern, log)
    else:
      if _file_name_match_patern(pattern, file_name):
        _download_ftp_file(ftp, item, item, overwrite, log)


def download_ftp_tree(ftp: ftplib.FTP, file_path: str, remote_path: str,
                      log: logging.Logger, pattern: str = None,
                      overwrite: bool = False,
                      guess_by_extension: bool = True):
  """Downloads an entire directory tree from an ftp server to the
  videos directory.
  """
  file_path = file_path.lstrip("/")

  original_directory = os.getcwd()
  os.chdir(remote_path)

  _mirror_ftp_dir(ftp, file_path, log=log, pattern=pattern,
                  overwrite=overwrite, guess_by_extension=guess_by_extension)

  os.chdir(original_directory)


def batch_download_from_ftp(username: str,
                            password: str,
                            public_address: str,
                            remote_path: str,
                            log: logging.Logger,
                            download_path: str = videos) -> Tuple:
  """Download/fetch/transfer file using OpenSSH via FTP.

  Fetch file from remote machine to store it in videos folder.

  Args:
    username: Username of the remote machine.
    password: Password of the remote machine.
    public_address: Remote server IP address.
    remote_file: Remote file to be downloaded/transferred.
    file_name: Filename for the downloaded file.
    log: Logger object for logging the status.
    download_path: Path (default: ./videos/) for saving file.

  Returns:
    Boolean value if the file is downloaded or not.
  """
  if check_file.match(remote_path):
    status, file = download_using_ftp(username, password, public_address,
                                       remote_path, log, download_path)
    return status, file
  else:
    # pyright: reportMissingImports=false
    from app.email import email_to_admin_for_FTP_urls

    ftp_files, urls = [], []
    parts = public_address.split('.')
    addr_type = len(parts) == 4 and all(0 <= int(part) < 256 for part in parts)

    try:
      if addr_type:
        os.system(f'sshpass -p {password} scp -r '
                  f'{username}@{public_address}:{remote_path} '
                  f'{download_path}')
        log.info('File(s) transfer from remote directory successful.')

        if remote_path.startswith('/'):
          remote_path = remote_path[1:]

        if remote_path.endswith('/'):
          remote_path = remote_path[:-1]

        transferred = os.path.join(download_path, os.path.basename(remote_path))
        ftp_files.extend([idx for idx in glob.glob(f'{transferred}/**',
                                                  recursive=True)
                                                  if idx.endswith
                                                  (video_file_extensions)])
      else:
        ftp = ftplib.FTP(public_address, username, password)
        download_ftp_tree(ftp, remote_path, download_path, log)

        if remote_path.startswith('/'):
          remote_path = remote_path[1:]

        if remote_path.endswith('/'):
          remote_path = remote_path[:-1]

        transferred = os.path.join(download_path, remote_path)
        ftp_files.extend([idx for idx in glob.glob(f'{transferred}/**',
                                                   recursive=True)
                                                   if os.path.isfile(idx)])
      tup = (["Sr.No.", "Video File Url"],)
      for _idx, _file in enumerate(ftp_files):
        list = []
        s3 = _file.split(download_path)[1]
        url = upload_to_bucket(_AWS_ACCESS_KEY, _AWS_SECRET_KEY,
                               'ftp-batch-downloaded-bucket', _file, log, s3)
        urls.append(url)
        list.append((_idx)+1)
        list.append(url)
        tup += (list,)
        log.info(f'Uploaded {_idx + 1}/{len(ftp_files)} > '
               f'{os.path.basename(_file)} on to S3 bucket.')
      html = generateHtml(tup)
      email_to_admin_for_FTP_urls(html)
      return True, urls
    except OSError:
      log.error('Directory transfer via FTP failed because of poor network '
                'connectivity.')
      return None, '[e] Error while transferring directory'


def generateHtml(data):
  html = "<table width='100%' cellpadding=4 cellspacing=1 border=1>"
  for j, Video_urls in data:
    if j == "Sr.No.":
      html += "<tr>"
      html += "<th>"+str(j)+"</th>"
      html += "<th>"+str(Video_urls)+"</th>"
      html += "</tr>"
    else:
      html += "<tr>"
      html += "<td>"+str(j)+"</td>"
      html += "<td>"+str(Video_urls)+"</td>"
      html += "</tr>"
  html += "</table>"
  return html


def earthcam_specific_download(username: str,
                               password: str,
                               public_address: str,
                               static_path: str,
                               start_date: Union[float, int, str],
                               start_hour: Union[float, int, str],
                               end_hour: Union[float, int, str],
                               log: logging.Logger,
                               file_name: str,
                               download_path: str = videos) -> Tuple:
  """
  Download/fetch/transfer file using FTP for EarthCam videos.

  Args:
    username: Username of the remote machine.
    password: Password of the remote machine.
    public_address: Remote server IP address.
    static_path: Remote static path to be downloaded/transferred.
    log: Logger object for logging the status.
    download_path: Path (default: ./videos/) for saving file.

  Returns:
    Boolean value if the file is downloaded or not.
  """
  try:
    start_date = f"{start_date} 00:00:00"
    prev = (datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S") +
            timedelta(days=-1))
    day = datetime.strptime(str(prev), "%Y-%m-%d %H:%M:%S").day
    month = datetime.strptime(str(prev), "%Y-%m-%d %H:%M:%S").month
    start_hour = str(start_hour).split(":")[0]
    end_hour = str(end_hour).split(":")[0]
    day = f'{day:02d}'
    month = f'{month:02d}'
    hours = range(int(start_hour), int(end_hour))
    temp = os.path.join(download_path, str(uuid4()))

    if not os.path.exists(temp):
      os.mkdir(temp)

    for hour in hours:
      file = os.path.join(static_path, month, day, f'{hour:>02}00.mp4')
      log.info(f"Fetching file {file}...")
      _, file = download_using_ftp(username, password, public_address, file,
                                  log, temp)

    log.info("Concatenating fetching videos...")
    output = concate_videos(temp)
    main_file = os.path.join(download_path, f'{file_name}.mp4')
    log.warning("Cleaning directory...")
    shutil.move(output, main_file)
    os.rmdir(temp)
    return True, main_file
  except Exception:
    return None, 'Remote file is not a media file.'
