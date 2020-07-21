"""Utility for defining the necessary paths."""

import os

# Parent directory path. All the references will be made relatively
# using the below defined parent directory.
parent_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

# Path where all the downloaded files are stored.
videos = os.path.join(parent_path, 'videos')

# Other paths
logs = os.path.join(parent_path, 'logs')
