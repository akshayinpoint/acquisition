"""Utility for upload files to Azure, GCS and FTP."""

import logging
import os
from typing import Optional


def push_to_client_ftp(username: str,
                       password: str,
                       public_address: str,
                       file_path: str,
                       remote_path: str,
                       log: logging.Logger) -> Optional[bool]:
  """Upload/push file using OpenSSH via FTP.

  Push file from current machine to a remote machine.

  Args:
    username: Username of the remote machine.
    password: Password of the remote machine.
    public_address: Remote server IP address.
    file_path: File to be pushed onto remote machine.
    remote_path: Remote path where the file is to be transferred.

  Returns:
    Boolean value if the file is uploaded or not.
  """
  # You can find the reference code here:
  # https://stackoverflow.com/a/56850195
  try:
    os.system(f'sshpass -p {password} scp -o StrictHostKeyChecking=no '
              f'{file_path} {username}@{public_address}:{remote_path}')
    log.info(f'File "{os.path.basename(file_path)}" transferred successfully.')
    return True
  except OSError:
    log.error('File transfer via FTP failed because of poor network '
              'connectivity.')
    return None
