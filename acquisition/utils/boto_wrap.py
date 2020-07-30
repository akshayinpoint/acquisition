"""Utility for work as a wrapper around Amazon's Boto3 API."""

import csv
import itertools
import logging
import math
import os
import time
from collections import defaultdict
from datetime import datetime
from typing import List, Optional, Tuple, Union

import boto3
import pytz
from botocore.exceptions import ClientError, NoCredentialsError
from botocore.utils import calculate_tree_hash

from acquisition.utils.common import check_internet, file_size
from acquisition.utils.logs import log
from acquisition.utils.paths import videos

video_file_extensions = ('.3gp', '.mp4', '.avi', '.webm', '.divx', '.f4v',
                         '.flv', '.m4v', '.mpg', '.mts', '.mxf', '.ogm', '.qt',
                         '.vob', '.wmv', '.3g2', '.3gpp', '.mov', '.mkv',
                         '.h265', '.ogv', '.ts', '.dat', '.m2ts', '.m2v',
                         '.3GP', '.MP4', '.AVI', '.WEBM', '.DIVX', '.F4V',
                         '.FLV', '.M4V', '.MPG', '.MTS', '.MXF', '.OGM', '.QT',
                         '.VOB', '.WMV', '.3G2', '.3GPP', '.MOV', '.MKV',
                         '.OGV', '.TS', '.DAT', '.M2TS', '.M2V', '.H265',
                         '.265')


def create_s3_bucket(access_key: str,
                     secret_key: str,
                     bucket_name: str,
                     log: logging.Logger,
                     region: str = 'ap-south-1') -> bool:
  """Create an S3 bucket.

  Create an S3 bucket in a specified region.
  If a region is not specified, the bucket is created in the S3 default
  region is 'ap-south-1 [Asia Pacific (Mumbai)]'.

  Args:
    access_key: AWS access key.
    secret_key: AWS secret key.
    bucket_name: Bucket to create.
    log: Logger object for logging the status.
    region: Bucket region (default: ap-south-1 [Asia Pacific (Mumbai)]).

  Returns:
    Boolean value, True if bucket created.
  """
  try:
    s3 = boto3.client('s3',
                      aws_access_key_id=access_key,
                      aws_secret_access_key=secret_key,
                      region_name=region)
  except (ClientError, NoCredentialsError):
    log.error('Wrong credentials used to access the AWS account.')
    return False
  else:
    location = {'LocationConstraint': region}
    try:
      s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
      log.info('New bucket created on Amazon S3 storage.')
    except:
      log.warning('Bucket already exist, skipped bucket creation.')
    return True


def upload_to_bucket(access_key: str,
                     secret_key: str,
                     bucket_name: str,
                     filename: str,
                     log: logging.Logger,
                     s3_name: str = None,
                     directory: str = None) -> Optional[str]:
  """Upload file to S3 bucket.

  Uploads file to the S3 bucket and returns it's public IP address.

  Args:
    access_key: AWS access key.
    secret_key: AWS saccess_key: str,
    bucket_name: Bucket to upload to.
    filename: Local file to upload.
    log: Logger object for logging the status.
    s3_name: Name (default: None) for the uploaded file.

  Returns:
    Public IP address of the uploaded file.
  """
  try:
    s3 = boto3.client('s3',
                      aws_access_key_id=access_key,
                      aws_secret_access_key=secret_key)
  except (ClientError, NoCredentialsError):
    log.error('Wrong credentials used to access the AWS account.')
    return None
  else:
    if s3_name is None:
      try:
        s3_name = os.path.basename(filename)
      except FileNotFoundError:
        log.error('File not found.')
        return None

    s3_name = os.path.join(directory, s3_name) if directory else s3_name

    while True:
      if check_internet(log):
        s3.upload_file(filename, bucket_name, s3_name,
                        ExtraArgs={'ACL': 'public-read',
                                  'ContentType': 'video/mp4'})
        log.info(f'{s3_name} file uploaded on to Amazon S3 bucket.')
        break
      else:
        log.info('Internet not available. Retrying upload after 30 secs.')
        time.sleep(30.0)

    return generate_s3_url(bucket_name, s3_name)

def generate_s3_url(bucket_name: str, s3_name: str) -> str:
  """Generate public url.

  Generates public url for accessing the uploaded file.

  Args:
    bucket_name: Bucket where file exists.
    s3_name: File name whose URL is to be fetched.

  Returns:
    String, public url.
  """
  s3_name = s3_name.replace(' ', '+')
  return f'https://{bucket_name}.s3.amazonaws.com/{s3_name}'


def check_file(access_key: str,
               secret_key: str,
               s3_url: str,
               log: logging.Logger,
               bucket_name: str = None) -> Optional[List]:
  """Return boolean status, bucket and filename.

  Checks if the file is available on S3 bucket and returns bucket and
  filename.

  Args:
    access_key: AWS access key.
    secret_key: AWS saccess_key: str,
    s3_url: Public url for the file.
    log: Logger object for logging the status.
    bucket_name: Bucket (default: None) to search and download from.

  Returns:
    List of boolean status, bucket and filename.
  """
  try:
    s3 = boto3.client('s3',
                      aws_access_key_id=access_key,
                      aws_secret_access_key=secret_key)
  except (ClientError, NoCredentialsError):
    log.error('Wrong credentials used to access the AWS account.')
    return None
  else:
    if bucket_name is None:
      bucket_name = s3_url.split('//')[1].split('.')[0]
    s3_file = s3_url.split('.amazonaws.com/')[1]
    return ['Contents' in s3.list_objects(Bucket=bucket_name,
                                          Prefix=s3_file),
            bucket_name,
            s3_file]


def access_file(access_key: str,
                secret_key: str,
                s3_url: str,
                file_name: str,
                log: logging.Logger,
                bucket_name: str = None) -> Tuple:
  """Access file from S3 bucket.

  Access and download file from S3 bucket.

  Args:
    access_key: AWS access key.
    secret_key: AWS saccess_key: str,
    s3_url: Public url for the file.
    log: Logger object for logging the status.
    bucket_name: Bucket to search and download from.

  Notes:
    This function ensures the file exists on the S3 bucket and then
    downloads the same. If the file doesn't exist on S3, it'll return
    None.
  """
  try:
    s3 = boto3.client('s3',
                      aws_access_key_id=access_key,
                      aws_secret_access_key=secret_key)
  except (ClientError, NoCredentialsError):
    log.error('Wrong credentials used to access the AWS account.')
    return None, '[e] Error while downloading file'
  else:
    [*status, bucket, file] = check_file(access_key, secret_key,
                                         s3_url, log, bucket_name)

    if status[0]:
      s3.download_file(bucket,
                       file,
                       os.path.join(videos, f'{file_name}.mp4'))
      log.info(f'File "{file_name}.mp4" downloaded from Amazon S3 storage.')

      if file_size(os.path.join(videos, f'{file_name}.mp4')).endswith('KB'):
        log.error('Unusable file downloaded since file size is in KBs.')
        return None, '[w] Unusable file downloaded.'

      return True, os.path.join(videos, f'{file_name}.mp4')
    else:
      log.error('File download from Amazon S3 failed because of poor network '
                'connectivity.')
      return None, '[e] Error while downloading file'


def save_file(bucket_name: str, filename: str) -> str:
  """Save S3 file.

  Create a directory with bucket name and save the file in it.

  Args:
    bucket_name: Bucket to search and download from.
    filename: File to download and save.

  Returns:
    String path where the downloaded file needs to be saved.

  Notes:
    If the `./videos/` folder doesn't exists, it creates one and
    proceeds further with it.
  """
  if not os.path.isdir(os.path.join(videos, bucket_name)):
    os.makedirs(os.path.join(videos, bucket_name))
  return os.path.join(os.path.join(videos, bucket_name), filename)


def copy_file_from_bucket(access_key: str,
                          secret_key: str,
                          customer_bucket_name: str,
                          customer_obj_key: str,
                          bucket_name: str,
                          log: logging.Logger,
                          bucket_obj_key: str = None) -> Optional[bool]:
  """Copy an object from one S3 bucket to another.

  Copies an object/file from one S3 bucket to another considering we
  have access.

  Args:
    access_key: AWS access key.
    secret_key: AWS saccess_key: str,
    customer_bucket_name: Bucket name from where we need to fetch file.
    customer_obj_key: Object/File name to be fetched.
    bucket_name: Target bucket name where to dump the fetched file.
    log: Logger object for logging the status.
    bucket_obj_key: Object name to be renamed in destination bucket.

  Notes:
    This function assumes that the files from 'customer_bucket_name'
    are publicly available.
  """
  try:
    s3 = boto3.resource('s3',
                        aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key)
  except (ClientError, NoCredentialsError):
    log.error('Wrong credentials used to access the AWS account.')
    return None
  else:
    copy_source = {
        'Bucket': customer_bucket_name,
        'Key': customer_obj_key
    }
    s3.meta.client.copy(copy_source, bucket_name, bucket_obj_key)
    return True


def create_glacier_vault(access_key: str,
                         secret_key: str,
                         account_id: str,
                         vault_name: str,
                         log: logging.Logger,
                         region: str = 'ap-south-1') -> bool:
  """Create a S3 Glacier vault.

  Create a S3 Glacier vault for archiving data in a specified region.
  If a region is not specified, the bucket is created in the S3 default
  region is 'ap-south-1 [Asia Pacific (Mumbai)]'.

  Args:
    access_key: AWS access key.
    secret_key: AWS secret key.
    account_id: AWS account ID.
    vault_name: Vault to create.
    log: Logger object for logging the status.
    region: Bucket region (default: ap-south-1 [Asia Pacific (Mumbai)]).

  Returns:
    Boolean value, True if the vault is created.
  """
  try:
    glacier = boto3.resource('glacier',
                             aws_access_key_id=access_key,
                             aws_secret_access_key=secret_key,
                             region_name=region)
  except (ClientError, NoCredentialsError):
    log.error('Wrong credentials used to access the AWS account.')
    return False
  else:
    glacier.Vault(account_id, vault_name).create()
    log.info('Vault created on Amazon S3 Glacier.')
    return True


def upload_to_vault(access_key: str,
                    secret_key: str,
                    vault_name: str,
                    file_name: str,
                    log: logging.Logger = None,
                    archive_name: str = None,
                    region: str = 'ap-south-1') -> Optional[dict]:
  """Upload archive to S3 Glacier.

  Uploads files to S3 Glacier for archival.

  Args:
    access_key: AWS access key.
    secret_key: AWS saccess_key: str,
    bucket_name: Bucket to upload to.
    file_name: Local file to upload.
    log: Logger object for logging the status.
    s3_name: Name (default: None) for the uploaded file.

  Returns:
    Dictionary/Response of the uploaded archived file.
  """
  # You can find the reference code here:
  # https://stackoverflow.com/a/52602270
  try:
    glacier = boto3.client('glacier',
                           aws_access_key_id=access_key,
                           aws_secret_access_key=secret_key,
                           region_name=region)
  except (ClientError, NoCredentialsError):
    log.error('Wrong credentials used to access the AWS account.')
    return None
  else:
    if archive_name is None:
      try:
        archive_name = os.path.basename(file_name)
      except FileNotFoundError:
        log.error('File not found.')
        return None

    upload_chunk = 2 ** 25
    mp_upload = glacier.initiate_multipart_upload
    mp_part = glacier.upload_multipart_part
    cp_upload = glacier.complete_multipart_upload
    multipart_archive_upload = mp_upload(vaultName=vault_name,
                                         archiveDescription=file_name,
                                         partSize=str(upload_chunk))

    file_size = os.path.getsize(file_name)
    multiple_parts = math.ceil(file_size / upload_chunk)

    with open(file_name, 'rb') as upload_archive:
      for idx in range(multiple_parts):
        min_size = idx * upload_chunk
        max_size = min_size + upload_chunk - 1

        if max_size > file_size:
          max_size = (file_size - min_size) + min_size - 1
        file_part = upload_archive.read(upload_chunk)
        mp_part(vaultName=vault_name,
                uploadId=multipart_archive_upload['uploadId'],
                range=f'bytes {min_size}-{max_size}/{file_size}',
                body=file_part)

    checksum = calculate_tree_hash(open(file_name, 'rb'))
    complete_upload = cp_upload(vaultName=vault_name,
                                uploadId=multipart_archive_upload['uploadId'],
                                archiveSize=str(file_size),
                                checksum=checksum)

    log.info(f'"{file_name}" file archived on AWS S3 Glacier.')
    return complete_upload


def access_limited_files(access_key: str,
                         secret_key: str,
                         bucket_name: str,
                         access_from: str,
                         access_to: str,
                         log: logging.Logger,
                         timestamp_format: str = '%Y-%m-%d %H:%M:%S') -> List:
  """Access files from S3 bucket for particular timeframe.

  Access and download file from S3 bucket for particular timeframe.

  Args:
    access_key: AWS access key.
    secret_key: AWS saccess_key: str,
    bucket_name: Bucket to search and download from.
    access_from: Datetime from when to start fetching files.
    access_to: Datetime till when to fetch files.
    log: Logger object for logging the status.
    timestamp_format: Timestamp format (default: %Y-%m-%d %H:%M:%S)

  Returns:
    List of the directories which hosts the downloaded files.
  """
  _glob = []
  try:
    s3 = boto3.client('s3',
                      aws_access_key_id=access_key,
                      aws_secret_access_key=secret_key)
  except (ClientError, NoCredentialsError):
    log.error('Wrong credentials used to access the AWS account.')
    return []
  else:
    limit_from = datetime.strptime(
        access_from, timestamp_format).replace(tzinfo=pytz.UTC)
    limit_till = datetime.strptime(
        access_to, timestamp_format).replace(tzinfo=pytz.UTC)
    bucket_dir = os.path.join(videos, bucket_name)
    concate_dir = []
    files_with_timestamp = {}

    all_files = s3.list_objects_v2(Bucket=bucket_name)
    unsupported = [idx['Key']
                   for idx in all_files['Contents']
                   if not idx['Key'].endswith(video_file_extensions)]
    unsupported = list(set(map(lambda x: os.path.splitext(x)[1], unsupported)))
    unsupported = [idx for idx in unsupported if idx != '']

    if len(unsupported) > 1:
      log.info(f'Unsupported video formats like "{unsupported[0]}", '
               f'"{unsupported[1]}", etc. will be skipped.')
    else:
      log.info(f'Files ending with "{unsupported[0]}" will be skipped.')

    for files in all_files['Contents']:
      if files['Key'].endswith(video_file_extensions):
        files_with_timestamp[files['Key']] = files['LastModified']

    sorted_files = sorted(files_with_timestamp.items(), key=lambda xa: xa[1])

    for file, timestamp in sorted_files:
      if timestamp > limit_from and timestamp < limit_till:
        s3_style_dir = os.path.join(bucket_dir, os.path.dirname(file))
        concate_dir.append(s3_style_dir)
        if not os.path.isdir(s3_style_dir):
          os.makedirs(s3_style_dir)
        s3.download_file(bucket_name, file,
                         os.path.join(s3_style_dir, os.path.basename(file)))
        log.info(f'File "{file}" downloaded from Amazon S3.')
        _glob.append(os.path.join(s3_style_dir, os.path.basename(file)))

    if len(concate_dir) > 0:
      sizes = [file_size(s_idx) for s_idx in _glob]
      temp = [(n, s) for n, s in zip(_glob, sizes)]
      with open(os.path.join(bucket_dir, f'{bucket_name}.csv'), 'a',
                encoding="utf-8") as csv_file:
        log.info('Logging downloaded files into a CSV file.')
        _file = csv.writer(csv_file, quoting=csv.QUOTE_MINIMAL)
        _file.writerow(['Files', 'Size on disk'])
        _file.writerows(temp)
      return list(set(concate_dir))

    else:
      return []


def analyze_storage_consumed(access_key: str,
                             secret_key: str,
                             log: logging.Logger,
                             customer_id: Union[int, str],
                             contract_id: Union[int, str] = None,
                             order_id: Union[int, str] = None) -> str:
  """Analyze storage.

  Analyze storage consumed by the customer in GBs.

  Args:
    access_key: AWS access key.
    secret_key: AWS saccess_key: str,
    customer_id: Customer whose storage size needs to be calculated.

  Returns:
    Storage file size of a customer on S3.
  """
  try:
    s3 = boto3.resource('s3',
                        aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key)
  except (ClientError, NoCredentialsError):
    log.error('Wrong credentials used to access the AWS account.')
    return 'Error'
  else:
    # pyright: reportGeneralTypeIssues=false
    customer_id = f'{customer_id:>04}'
    contract_id = f'{contract_id:>02}' if contract_id else None
    order_id = f'{order_id:>02}' if order_id else None

    log.debug(f'Calculating storage size used by "{customer_id}"...')

    all_buckets = [bucket.name for bucket in s3.buckets.all()]
    all_customers = list(map(lambda x: x[2:].isdigit()
                             if len(x) == 6 else False,
                             all_buckets))

    valid_customers = list(itertools.compress(all_buckets, all_customers))
    customers = [(idx[2:6], idx) for idx in valid_customers]
    customer = defaultdict(list)

    for k, v in customers:
      customer[k].append(v)

    size = 0
    bucket = s3.Bucket(customer[customer_id][0])

    for obj in bucket.objects.all():
      if not contract_id:
        size += obj.size
      elif contract_id and order_id:
        if f'{customer_id}{contract_id}{order_id}' in obj.key:
          size += obj.size
      else:
        if f'{customer_id}{contract_id}' in obj.key:
          size += obj.size

    return f'{round((size * 100) / 5e+12, 5)}'
