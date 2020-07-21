import json
import threading
import time
from datetime import datetime, timedelta
from typing import List
from uuid import uuid4

from acquisition.core.bugsbunny import spin
from acquisition.utils.common import datetime_to_utc, now
from acquisition.utils.logs import log
# pyright: reportMissingImports=false
from app import models

deployed = False

if deployed:
  _log = log('error')
else:
  _log = log('info')

threads = threading.Semaphore(100)
orders = []


class SpawnTheSheep(object):

  def __init__(self):
    super(SpawnTheSheep, self).__init__()
    self.active = []
    self.lock = threading.Lock()

  def spawn(self, name: str):
    with self.lock:
      self.active.append(name)

  def despawn(self, name: str):
    with self.lock:
      self.active.remove(name)


pool = SpawnTheSheep()


def sheep(threads: threading.Semaphore, json_obj: dict, db_pk: int) -> None:
  """Sheep thread object.
  
  Args:
    threads: Thread semaphore object (thread count).
    json_obj: JSON dictionary which Admin sends to VPE.
    db_pk: Primary key of Database entry.
  """
  with threads:
    name = threading.currentThread().getName()
    pool.spawn(name)
    updated_date = None

    try:
      while True:
        start_time = json_obj['start_time']
        run_date = json_obj['start_date']
        end_date = json_obj['end_date']
        end_date = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)
        timezone = json_obj.get('camera_timezone', 'UTC')
        run_date = updated_date if updated_date else run_date
        _start_time = f'{run_date} {start_time}'
        _start_time = datetime_to_utc(_start_time,
                                      timezone,
                                      '%Y-%m-%d %H:%M:%S')
        _start_obj = datetime.strptime(_start_time, '%Y-%m-%d %H:%M:%S')
        _log.critical('Acquisition Engine is scheduled to start from '
                      f'{_start_time} till {end_date.date()} for order '
                      f'#{db_pk}.')

        while now().date() < end_date.date():
          if str(now()) == str(_start_time):
            status_db = models.RequestStatus.objects.filter(id=db_pk).values()
            status_db.update(processing_status_id=2)
            spin(json.dumps(json_obj), run_date, now(), _log, db_pk)
            status_db = models.RequestStatus.objects.filter(id=db_pk).values()
            status_db.update(processing_status_id=1)

            run_date = now() + timedelta(days=1)
            run_date = run_date.strftime('%Y-%m-%d')

            if json_obj.get("use_archived", False):
              if not json_obj["sub_json"].get("earthcam_download", False):
                pool.despawn(name)
                _log.warning(f'Thread "{name}" released.')

            if str(end_date.date()) == str(run_date):
              _log.critical('Acquisition Engine has stopped updating the next '
                            f'run cycle for order #{db_pk}.')
              status_db.update(processing_status_id=3)
              pool.despawn(name)
              _log.warning(f'Thread "{name}" released.')
            else:
              _log.critical('Acquisition Engine is updating the next run '
                            f'cycle for order #{db_pk}...')
              status_db.update(processing_status_id=1)
              updated_date = run_date
              break
    
          time.sleep(1.0)
    except KeyboardInterrupt:
      _log.error('Video processing engine sheep interrupted.')
      pool.despawn(name)
    except Exception as _error:
      _log.exception(_error)


def hill(orders: List):
  """Some threading related stuff."""
  for idx in orders:
    agl = ''.join([str(idx['customer_id']), str(idx['area_code']),
                   str(idx['camera_id']), str(uuid4())[:8]])
    request_db = models.RequestStatus(json=idx, thread_name=f'order_{agl}',
                                      processing_status_id=2)
    request_db.save()
    db_pk = request_db.id
    t = threading.Thread(target=sheep, name=f'order_{agl}',
                          args=(threads, idx, db_pk))
    t.start()
