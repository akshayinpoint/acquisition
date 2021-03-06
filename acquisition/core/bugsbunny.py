"""Complete video processing engine in one go."""

import json
import logging
import os
import time
from uuid import uuid4
from datetime import datetime

import requests

from acquisition.core.scoobydoo import live, stored
from acquisition.utils.common import now
from acquisition.utils.generate import bucket_name, order_name
from acquisition.utils.boto_wrap import upload_to_bucket
# pyright: reportMissingImports=false
from app import models

_AWS_ACCESS_KEY = 'XAMES3'
_AWS_SECRET_KEY = 'XAMES3'


def calling_processing(json_obj: str, log: logging.Logger) -> bool:
  """This is something which works with/on REST api."""
  try:
    header = {'api-key': ('epVgnissecorP2020yjbadsdsa05jdagdsah22a'
                          'll0ahm0duil0lah03333fo0r33eve0ryt0hin0g')}

    # UAT and Staging
    # URL = 'http://64.227.0.147:9000/new_connection_order/'
    # URL = 'http://127.0.0.1:9000/new_connection_order/'

    # Production
    URL = 'http://161.35.6.215:9000/new_connection_order/'

    requests.post(URL, json.dumps(json_obj), headers=header)

    return True
  except Exception as error:
    log.critical('Something went wrong while running calling_processing().')
    log.exception(error)
    return False


def spin(json_obj: str,
         run_date: str,
         current: datetime,
         log: logging.Logger,
         db_pk: int) -> None:
  """Spin the Video Processing Engine."""
  try:
    start = now()
    org_file = None

    json_data = json.loads(json_obj)
    log.info('Parsed consumer JSON request.')

    country = json_data.get('country_code', 'xa')
    customer = json_data.get('customer_id', 0)
    contract = json_data.get('contract_id', 0)
    order = json_data.get('order_id', 0)
    store = json_data.get('store_id', 0)
    area = json_data.get('area_code', 'e')
    camera = json_data.get('camera_id', 0)
    use_archived = json_data.get('use_archived', False)
    start_time = json_data['start_time']
    end_time = json_data['end_time']
    address = json_data['camera_address']
    username = json_data.get('camera_username', 'admin')
    password = json_data['camera_password']
    port = json_data.get('camera_port', 554)
    timeout = (json_data.get('camera_timeout', 30.0))
    timestamp = json_data.get('timestamp_format', '%H:%M:%S')

    _order = json_data.get('order_id', 0)
    bucket = bucket_name(country, customer, contract, order)
    order = order_name(store, area, camera, current)

    log.info(f'Aquisition Engine loaded.')
    log.info(f'Aquisition Engine started spinning for angle #{camera}...')

    if use_archived:
      log.info(f'Received CST: {customer}, CNT: {contract}, '
               f'ODR: {_order}, STR: {store}, AGL: {camera}, '
               f'SRC: {json_data["access_mode"]}, START: {start_time} '
               f'and END: {end_time}.')
      log.info('Aquisition mode selected: ARCHIVED')
      json_data['sub_json']['access_type'] = json_data['access_mode']
      json_data['sub_json']['earthcam_start_date'] = json_data['start_date']
      json_data['sub_json']['stored_filename'] = str(uuid4())

      log.info('Aquiring downloaded video for processing this order...')
      _, org_file = stored(json_data['sub_json'], log)
    else:
      log.info(f'Received CST: {customer}, CNT: {contract}, '
               f'ODR: {_order}, STR: {store}, AGL: {camera}, '
               f'SRC: {address}, START: {start_time} and END: {end_time}.')
      log.info('Aquisition mode selected: LIVE')
      log.info(f'Recording from camera #{camera} for this order...')
      org_file = live(bucket, order, run_date, start_time, end_time, address,
                      username, password, port, timeout, log, timestamp)

    # pyright: reportGeneralTypeIssues=false
    if not os.path.isfile(org_file):
      log.error('File not selected for processing.')
      return

    log.info('Updating Event Milestone 01 - Video Acquisition...')
    milestone_db = models.MilestoneStatus(work_status_id=db_pk,
                                          milestone_id=1)
    milestone_db.save()
    log.info('Event Milestone 01 - Video Acquisition: UPDATED')

    while True:
      log.info("Backing up the raw video on cloud...")
      json_data['db_pk'] = db_pk
      json_data['org_file'] = upload_to_bucket(_AWS_ACCESS_KEY,
                                               _AWS_SECRET_KEY,
                                               "archived-order-uploads",
                                               org_file,
                                               log,
                                               directory=bucket)
      trigger_status = calling_processing(json_data, log)
      if trigger_status:
        log.info('Trigger status: SUCCESSFULL')
        os.remove(org_file)
        break
      else:
        log.error('Trigger status: FAILED')
        time.sleep(timeout)

    log.info(f'Acquisition Engine took {now() - start} to acquire video.')
  except KeyboardInterrupt:
    log.error('Spinner interrupted.')
  except Exception as error:
    log.critical('Something went wrong while video processing was running.')
    log.exception(error)


def save_milestone(db_pk, stone_id) -> bool:
  try:
    milestone_db = models.MilestoneStatus(work_status_id=db_pk,
                                          milestone_id=stone_id)
    milestone_db.save()
    return True
  except Exception as error:
    print(error)
    return False
