"""Utility for generating strings and filename."""

import time
from datetime import datetime
from typing import Optional, Union

from acquisition.utils.hasher import h_17k, h_26, h_676, h_area, h_country


def hash_a(unique_id: Union[int, float, str]) -> Optional[str]:
  """Return hashed string code for single unique id from a - z.

  The unique id is fetched from the database and should range from 
  1 to 26 values.
  This function is ideal for hashing hours & months in a timestamp.

  Args:
    unique_id: Integer, float or string value from database.

  Returns:
    Hashed string from h_26 dictionary.

  Notes:
    Values greater than 26 will return None.
  """
  return h_26.get(int(unique_id), None)


def hash_aa(unique_id: Union[int, float, str]) -> Optional[str]:
  """Return hashed string code for the double unique ids from aa - zz.

  The unique id is fetched from the database and should range from 
  1 to 676 values. The hashing is done purely on the ideology of
  python dictionaries.
  This function is suitable for hashing values in range of 00-99.

  Args:
    unique_id: Integer, float or string value from database.

  Returns:
    Hashed string from h_676 dictionary.

  Notes:
    Values greater than 676 will return None.
  """
  return h_676.get(int(unique_id), None)


def hash_aaa(unique_id: Union[int, float, str],) -> Optional[str]:
  """Return hashed string code for single unique id from aaa - zzz.

  The unique id is fetched from the database and should range from 
  1 to 17576 values. Similar to `hash_aa()`, the hashing is done
  purely on the ideology of python dictionaries.
  This function is ideal for covering almost all the possible ranges
  for the customers.

  Args:
    unique_id: Integer, float or string value from database.

  Returns:
    Hashed string from h_17k dictionary.

  Notes:
    Values greater than 17576 will return None.
  """
  return h_17k.get(int(unique_id), None)


def hash_area_code(area: str) -> Optional[str]:
  """Return hashed string code.

  Args:
    area: Area to be hashed.

  Returns:
    Character representing the area.

  Notes:
    Refer documentation for the area code hashes.

  Raises:
    KeyError: If the key is not found.
    ValueError: If the value is not found.
  """
  # pyright: reportGeneralTypeIssues=false
  return dict(map(reversed, h_area.items()))[area]


def hash_country_code(country_code: str) -> str:
  """Return hashed country code."""
  return h_country.get(country_code, 'xa')


def hash_timestamp(now: datetime = None) -> str:
  """Return converted timestamp.

  Generate 'hashed' timestamp for provided instance in 'MMDDYYHHmmSS'.

  Args:
    now: Current timestamp (default: None).

  Returns:
    Hashed timestamp in MMDDYYHHmmSS.
  """
  if now is None:
    now = datetime.now().replace(microsecond=0)
  return '{}{:0>2}{:0>2}{}{:0>2}{:0>2}'.format(hash_a(now.month),
                                               str(now.day),
                                               str(now.year)[2:],
                                               hash_a(now.hour + 1),
                                               str(now.minute),
                                               str(now.second))


def bucket_name(country_code: str,
                customer_id: Union[int, float, str],
                contract_id: Union[int, float, str],
                order_id: Union[int, float, str]) -> str:
  """Generate an unique bucket name.

  The generated name represents the hierarchy of the stored video.

  Args:
    country_code: 2 letter country code (eg: India -> IN).
    customer_id: Customer Id from customer_id table from 1 - 9999.
    contract_id: Contract Id from contract_id table from 1 - 99.
    order_id: Order Id from order_id table from 1 - 99.

  Returns:
    Unique string name for S3 bucket.

  Raises:
    TypeError: If any positional arguments are skipped.
  """
  return '{}{:0>4}{:0>2}{:0>2}'.format(hash_country_code(country_code),
                                       int(customer_id),
                                       int(contract_id),
                                       int(order_id))


def order_name(store_id: Union[int, float, str],
               area_code: str,
               camera_id: Union[int, float, str],
               timestamp: Optional[datetime] = None,) -> str:
  """Generate an unique order name.

  Generate an unique string based on order details.

  Args:
    store_id: Store Id from store_id table from 1 - 99999.
    area_code: Area code from area_id table (p -> Parking lot).
    camera_id: Camera Id from camera_id table from 1 - 99.
    timestamp: Current timestamp (default: None).

  Returns:
    Unique string based on the order details.

  Raises:
    TypeError: If any positional arguments are skipped.
  """
  return '{:0>5}{}{:0>2}{}'.format(int(store_id), area_code,
                                   int(camera_id), hash_timestamp(timestamp))


def video_type(compress: bool = False,
               trim: bool = False,
               trim_compress: bool = False) -> str:
  """Return type of the video.

  The returned value is generated by conditional checks.

  Args:
    compress: Boolean value (default: False) if video to be compress.
    trim: Boolean value (default: False) if video is to be trimmed.
    trim_compress: Boolean value (default: False) if trimmed video is
                   to be compressed.

  Returns:
    String for video type.
  """
  temp = ['a', 'a']
  if compress:
      temp[0] = 'c'
  if trim:
      temp[1] = 'n'
      if trim_compress:
          temp[1] = 'c'
  return ''.join(temp)


def unhash_a(value: str) -> Optional[str]:
  """Return unhashed number from range 1 - 26.

  This function converts the `hashed string` value back to it's numeric
  form.

  Args:
    value: String to be unhashed.

  Returns:
    Unhashed number.

  Raises:
    KeyError: If an invalid value is passed for unhashing.
    ValueError: If the value to be unhashed is greater than the range.
  """
  return str(dict(map(reversed, h_26.items()))[value])


def unhash_aa(value: str) -> Optional[str]:
  """Return unhashed number from range 1 - 676.

  Similar to unhash_a(), this function converts the `hashed string`
  value back to it's numeric form.

  Args:
    value: String to be unhashed.

  Returns:
    Unhashed number.

  Raises:
    KeyError: If an invalid value is passed for unhashing.
    ValueError: If the value to be unhashed is greater than the range.
  """
  return str(dict(map(reversed, h_676.items()))[value])


def unhash_aaa(value: str) -> Optional[str]:
  """Return unhashed number from range 1 - 17576.

  Similar to unhash_a(), this function converts the `hashed string`
  value back to it's numeric form.

  Args:
    value: String to be unhashed.

  Returns:
    Unhashed number.

  Raises:
    KeyError: If an invalid value is passed for unhashing.
    ValueError: If the value to be unhashed is greater than the range.
  """
  return str(dict(map(reversed, h_17k.items()))[value])


def unhash_area_code(area_code: str) -> Optional[str]:
  """Return unhashed area code."""
  return h_area.get(area_code, None)


def unhash_country_code(hashed_code: str) -> Optional[str]:
  """Return unhashed country code."""
  return dict(map(reversed, h_country.items()))[hashed_code]


def unhash_timestamp(hashed_timestamp: str,
                     timestamp_format: str = '%m%d%y%H%M%S',
                     unix_time: bool = False) -> Union[datetime, float]:
  """Returns unhashed timestamp value.

  Returns the unhashed timestamp as per requirement.

  Args:
    hashed_timestamp: Hashed timestamp to unhash.
    timestamp_format: The format of hashed timestamp.
    unix_time: Boolean (default: False) value if unix time to be used.

  Returns:
    Datetime object or a Unix time (float) value of the hashed time.
  """
  temp = hashed_timestamp.replace(hashed_timestamp[0],
                                  unhash_a(hashed_timestamp[0]))
  temp = temp.replace(temp[5], str(int(unhash_a(hashed_timestamp[5])) - 1))
  if unix_time:
    return time.mktime(datetime.strptime(temp, timestamp_format).timetuple())
  else:
    return datetime.strptime(temp, timestamp_format)
